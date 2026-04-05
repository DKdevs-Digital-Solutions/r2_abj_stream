import 'dotenv/config'
import Fastify from 'fastify'
import { Readable, Transform } from 'node:stream'
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3'
import { v4 as uuidv4 } from 'uuid'
import mime from 'mime-types'
import client from 'prom-client'

const SERVICE_NAME = 'r2-uploader'
const SERVICE_VERSION = process.env.npm_package_version || '1.4.0'
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

const register = new client.Registry()
register.setDefaultLabels({
  service: SERVICE_NAME,
  version: SERVICE_VERSION,
})
client.collectDefaultMetrics({ register, prefix: 'r2_uploader_' })

const uploadRequestsTotal = new client.Counter({
  name: 'r2_uploader_requests_total',
  help: 'Total de chamadas processadas pela rota de upload',
  labelNames: ['route', 'method', 'status_code', 'outcome'],
  registers: [register],
})

const uploadErrorsTotal = new client.Counter({
  name: 'r2_uploader_errors_total',
  help: 'Total de chamadas com erro na rota de upload',
  labelNames: ['route', 'method', 'status_code', 'outcome'],
  registers: [register],
})

const uploadDurationSeconds = new client.Histogram({
  name: 'r2_uploader_request_duration_seconds',
  help: 'Tempo de resposta da rota de upload em segundos',
  labelNames: ['route', 'method', 'status_code', 'outcome'],
  buckets: [0.1, 0.3, 0.5, 1, 2, 5, 10, 30, 60, 120],
  registers: [register],
})

const uploadedBytesTotal = new client.Counter({
  name: 'r2_uploader_uploaded_bytes_total',
  help: 'Total de bytes enviados com sucesso para o R2',
  labelNames: ['route', 'method'],
  registers: [register],
})

const inflightRequests = new client.Gauge({
  name: 'r2_uploader_inflight_requests',
  help: 'Quantidade de uploads em andamento',
  labelNames: ['route', 'method'],
  registers: [register],
})

app.get('/', async () => buildHealthPayload())
app.get('/health', async () => buildHealthPayload())
app.get('/metrics', async (_req, reply) => {
  reply.header('Content-Type', register.contentType)
  return register.metrics()
})

app.post(ROUTE_UPLOAD, async (req, reply) => {
  const startedAt = process.hrtime.bigint()
  const metricLabelsBase = {
    route: ROUTE_UPLOAD,
    method: 'POST',
  }

  inflightRequests.inc(metricLabelsBase)

  let timeout = null
  let finalized = false

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

    const durationSeconds = Number(process.hrtime.bigint() - startedAt) / 1e9
    const outcome = statusCode >= 500 ? 'server_error' : statusCode >= 400 ? 'client_error' : 'success'
    const metricLabels = {
      ...metricLabelsBase,
      status_code: String(statusCode),
      outcome,
    }

    uploadRequestsTotal.inc(metricLabels)
    uploadDurationSeconds.observe(metricLabels, durationSeconds)
    if (statusCode >= 400) {
      uploadErrorsTotal.inc(metricLabels)
    }
    if (bytes > 0 && statusCode < 400) {
      uploadedBytesTotal.inc(metricLabelsBase, bytes)
    }

    req.log[logLevel](
      {
        route: ROUTE_UPLOAD,
        statusCode,
        durationMs: Number((durationSeconds * 1000).toFixed(2)),
        sourceHost,
        sourceStatus,
        contentType,
        fileUrl,
        responsePayload: payload,
      },
      statusCode < 400 ? 'Upload concluído' : 'Upload finalizado com erro'
    )

    inflightRequests.dec(metricLabelsBase)
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
        'User-Agent': 'r2-uploader/1.4 (+Prometheus)',
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
      metricsPath: '/metrics',
      prometheus: true,
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
