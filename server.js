import 'dotenv/config'
import Fastify from 'fastify'
import { Readable, Transform } from 'node:stream'
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3'
import { v4 as uuidv4 } from 'uuid'
import mime from 'mime-types'

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

app.get('/health', async () => ({ status: 'ok' }))

app.post('/upload-from-url', async (req, reply) => {
  try {
    const blobUrl = req.body?.blobUrl

    if (!blobUrl) {
      return reply.code(400).send({ error: 'blobUrl é obrigatório' })
    }

    let parsedUrl
    try {
      parsedUrl = new URL(blobUrl)
    } catch {
      return reply.code(400).send({ error: 'blobUrl inválida' })
    }

    if (!['http:', 'https:'].includes(parsedUrl.protocol)) {
      return reply.code(400).send({ error: 'blobUrl deve usar http ou https' })
    }

    const controller = new AbortController()
    const timeout = setTimeout(() => {
      controller.abort(new Error('Tempo limite excedido ao processar upload'))
    }, config.uploadTimeoutMs)

    try {
      const response = await fetch(blobUrl, {
        method: 'GET',
        signal: controller.signal,
        redirect: 'follow',
      })

      if (!response.ok) {
        return reply.code(400).send({ error: `Falha ao baixar arquivo: HTTP ${response.status}` })
      }

      if (!response.body) {
        throw new Error('Origem não retornou conteúdo para download')
      }

      const contentLengthHeader = response.headers.get('content-length')
      const contentLength = contentLengthHeader ? Number(contentLengthHeader) : null

      if (Number.isFinite(contentLength) && contentLength > config.maxFileSizeBytes) {
        return reply.code(413).send({
          error: `Arquivo excede o limite permitido de ${config.maxFileSizeBytes} bytes`,
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

      return reply.send({
        url: fileUrl,
        contentType,
        bytes: transferredBytes || contentLength || 0,
      })
    } finally {
      clearTimeout(timeout)
    }
  } catch (err) {
    req.log.error({ err }, 'Erro ao processar upload')

    if (err?.name === 'AbortError') {
      return reply.code(504).send({ error: 'Tempo limite excedido ao baixar ou enviar arquivo' })
    }

    if (err?.message?.includes('Arquivo excede o limite permitido')) {
      return reply.code(413).send({ error: err.message })
    }

    return reply.code(500).send({ error: 'Erro interno ao processar upload' })
  }
})

start()

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
