import 'dotenv/config'
import Fastify from 'fastify'
import { Readable, Transform } from 'node:stream'
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3'
import { v4 as uuidv4 } from 'uuid'
import mime from 'mime-types'
import swaggerStats from 'swagger-stats'

const SERVICE_NAME = 'r2-uploader'
const SERVICE_VERSION = process.env.npm_package_version || '1.5.0'
const ROUTE_UPLOAD = '/upload-from-url'

const config = {
  port: Number(process.env.PORT || 3000),
  host: '0.0.0.0',
  publicUrl: requiredEnv('PUBLIC_URL'),
  r2Endpoint: requiredEnv('R2_ENDPOINT'),
  r2AccessKey: requiredEnv('R2_ACCESS_KEY'),
  r2SecretKey: requiredEnv('R2_SECRET_KEY'),
  r2Bucket: requiredEnv('R2_BUCKET'),
  uploadTimeoutMs: Number(process.env.UPLOAD_TIMEOUT_MS || 600000),
  requestTimeoutMs: Number(process.env.REQUEST_TIMEOUT_MS || 610000),
  maxFileSizeBytes: Number(process.env.MAX_FILE_SIZE_BYTES || 536870912),
}

const app = Fastify({
  logger: true,
  requestTimeout: config.requestTimeoutMs,
  bodyLimit: 1024 * 1024,
})

const s3 = new S3Client({
  region: 'auto',
  endpoint: config.r2Endpoint,
  credentials: {
    accessKeyId: config.r2AccessKey,
    secretAccessKey: config.r2SecretKey,
  },
  requestChecksumCalculation: 'WHEN_REQUIRED',
  responseChecksumValidation: 'WHEN_REQUIRED',
})

const swaggerSpec = {
  swagger: '2.0',
  info: {
    title: 'R2 Uploader API',
    version: SERVICE_VERSION,
    description: 'API para baixar arquivos por URL e enviar ao Cloudflare R2.',
  },
  basePath: '/',
  schemes: ['http', 'https'],
  paths: {
    '/': {
      get: {
        summary: 'Resumo de saúde da API',
        responses: {
          200: { description: 'API disponível' },
        },
      },
    },
    '/health': {
      get: {
        summary: 'Healthcheck da API',
        responses: {
          200: { description: 'API saudável' },
        },
      },
    },
    [ROUTE_UPLOAD]: {
      post: {
        summary: 'Baixa um arquivo e envia ao Cloudflare R2',
        consumes: ['application/json'],
        parameters: [
          {
            in: 'body',
            name: 'body',
            required: true,
            schema: {
              type: 'object',
              required: ['blobUrl'],
              properties: {
                blobUrl: {
                  type: 'string',
                  example: 'https://example.com/file.pdf',
                },
              },
            },
          },
        ],
        responses: {
          200: { description: 'Upload concluído com sucesso' },
          400: { description: 'Requisição inválida' },
          401: { description: 'Origem exige autenticação' },
          403: { description: 'Origem recusou o download' },
          404: { description: 'Arquivo não encontrado na origem' },
          413: { description: 'Arquivo maior que o limite configurado' },
          429: { description: 'Origem limitou a requisição' },
          500: { description: 'Erro interno' },
          502: { description: 'Erro da origem durante o download' },
          504: { description: 'Timeout durante download ou upload' },
        },
      },
    },
  },
}

await app.register(async function monitoredRoutes(fastify) {
  await fastify.register(swaggerStats.getFastifyPlugin, {
    swaggerSpec,
    uriPath: '/swagger-stats',
    name: SERVICE_NAME,
  })

  fastify.get('/', async () => buildHealthPayload())
  fastify.get('/health', async () => buildHealthPayload())

  fastify.post(ROUTE_UPLOAD, async (req, reply) => {
    let timeout = null
    let finalized = false
    const startedAt = process.hrtime.bigint()

    const finalize = ({
      statusCode,
      payload,
      sourceHost,
      sourceStatus,
      bytes = 0,
      contentType,
      fileUrl,
      logLevel = 'info',
    }) => {
      if (finalized) {
        return reply
      }
      finalized = true

      if (timeout) {
        clearTimeout(timeout)
      }

      const durationMs = Number(process.hrtime.bigint() - startedAt) / 1e6

      req.log[logLevel](
        {
          route: ROUTE_UPLOAD,
          statusCode,
          durationMs: Number(durationMs.toFixed(2)),
          sourceHost,
          sourceStatus,
          contentType,
          bytes,
          fileUrl,
          responsePayload: payload,
        },
        statusCode < 400 ? 'Upload concluído' : 'Upload finalizado com erro'
      )

      return reply.code(statusCode).send(payload)
    }

    try {
      const blobUrl = req.body?.blobUrl

      if (!blobUrl) {
        return finalize({
          statusCode: 400,
          payload: { error: 'blobUrl é obrigatório' },
          logLevel: 'warn',
        })
      }

      let parsedUrl
      try {
        parsedUrl = new URL(blobUrl)
      } catch {
        return finalize({
          statusCode: 400,
          payload: { error: 'blobUrl inválida' },
          logLevel: 'warn',
        })
      }

      if (!['http:', 'https:'].includes(parsedUrl.protocol)) {
        return finalize({
          statusCode: 400,
          payload: { error: 'blobUrl deve usar http ou https' },
          logLevel: 'warn',
          sourceHost: parsedUrl.host,
        })
      }

      const controller = new AbortController()
      timeout = setTimeout(() => {
        controller.abort(new Error('Tempo limite excedido ao processar upload'))
      }, config.uploadTimeoutMs)

      const response = await fetch(blobUrl, {
        method: 'GET',
        signal: controller.signal,
        redirect: 'follow',
        headers: {
          'User-Agent': 'r2-uploader/1.5 (+swagger-stats)',
          Accept: '*/*',
        },
      })

      if (!response.ok) {
        return finalize({
          statusCode: mapDownloadFailureStatus(response.status),
          payload: { error: `Falha ao baixar arquivo: HTTP ${response.status}` },
          logLevel: response.status >= 500 ? 'error' : 'warn',
          sourceHost: parsedUrl.host,
          sourceStatus: response.status,
        })
      }

      if (!response.body) {
        throw new Error('Origem não retornou conteúdo para download')
      }

      const contentLengthHeader = response.headers.get('content-length')
      const contentLength = contentLengthHeader ? Number(contentLengthHeader) : null

      if (Number.isFinite(contentLength) && contentLength > config.maxFileSizeBytes) {
        return finalize({
          statusCode: 413,
          payload: { error: `Arquivo excede o limite permitido de ${config.maxFileSizeBytes} bytes` },
          logLevel: 'warn',
          sourceHost: parsedUrl.host,
          sourceStatus: response.status,
        })
      }

      const contentType = response.headers.get('content-type') || 'application/octet-stream'
      const ext = resolveExtension({ contentType, blobUrl })
      const fileKey = `${uuidv4()}.${ext}`

      let transferredBytes = 0
      const byteCounter = new Transform({
        transform(chunk, _encoding, callback) {
          transferredBytes += chunk.length

          if (transferredBytes > config.maxFileSizeBytes) {
            callback(new Error(`Arquivo excede o limite permitido de ${config.maxFileSizeBytes} bytes`))
            return
          }

          callback(null, chunk)
        },
      })

      const uploadBody = Readable.fromWeb(response.body).pipe(byteCounter)

      const putObjectInput = {
        Bucket: config.r2Bucket,
        Key: fileKey,
        Body: uploadBody,
        ContentType: contentType,
      }

      if (Number.isFinite(contentLength) && contentLength >= 0) {
        putObjectInput.ContentLength = contentLength
      }

      await s3.send(new PutObjectCommand(putObjectInput))

      const fileUrl = new URL(fileKey, `${config.publicUrl.replace(/\/$/, '')}/`).toString()
      const responsePayload = {
        url: fileUrl,
        contentType,
        bytes: transferredBytes || contentLength || 0,
      }

      return finalize({
        statusCode: 200,
        payload: responsePayload,
        sourceHost: parsedUrl.host,
        sourceStatus: response.status,
        bytes: responsePayload.bytes,
        contentType,
        fileUrl,
      })
    } catch (err) {
      req.log.error({ err }, 'Erro ao processar upload')

      if (err?.name === 'AbortError') {
        return finalize({
          statusCode: 504,
          payload: { error: 'Tempo limite excedido ao baixar ou enviar arquivo' },
          logLevel: 'error',
        })
      }

      if (err?.message?.includes('Arquivo excede o limite permitido')) {
        return finalize({
          statusCode: 413,
          payload: { error: err.message },
          logLevel: 'warn',
        })
      }

      return finalize({
        statusCode: 500,
        payload: { error: 'Erro interno ao processar upload' },
        logLevel: 'error',
      })
    }
  })
})

await start()

async function start() {
  try {
    await app.listen({ port: config.port, host: config.host })
  } catch (err) {
    app.log.error(err)
    process.exit(1)
  }
}

function requiredEnv(name) {
  const value = process.env[name]
  if (!value) {
    throw new Error(`Variável de ambiente obrigatória não configurada: ${name}`)
  }
  return value
}

function resolveExtension({ contentType, blobUrl }) {
  const contentTypeWithoutCharset = contentType.split(';')[0].trim()
  const mimeExtension = mime.extension(contentTypeWithoutCharset)

  if (mimeExtension) {
    return mimeExtension
  }

  try {
    const pathname = new URL(blobUrl).pathname
    const fromPath = pathname.split('.').pop()
    if (fromPath && fromPath.length <= 10) {
      return fromPath.toLowerCase()
    }
  } catch {
    // noop
  }

  return 'bin'
}

function buildHealthPayload() {
  return {
    status: 'ok',
    service: SERVICE_NAME,
    version: SERVICE_VERSION,
    uptimeSeconds: Number(process.uptime().toFixed(2)),
    timestamp: new Date().toISOString(),
    monitoring: {
      healthPath: '/health',
      dashboardPath: '/swagger-stats/ui',
      statsPath: '/swagger-stats/stats',
      swaggerStats: true,
    },
  }
}

function mapDownloadFailureStatus(sourceStatus) {
  if (sourceStatus >= 500) {
    return 502
  }

  if ([401, 403, 404, 408, 410, 429].includes(sourceStatus)) {
    return sourceStatus
  }

  return 400
}
