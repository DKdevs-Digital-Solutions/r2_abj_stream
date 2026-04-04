# R2 Uploader

API Fastify para baixar um arquivo a partir de uma URL e subir diretamente para o Cloudflare R2, devolvendo a URL pública na mesma resposta.

## Melhorias aplicadas

- Upload em streaming para evitar carregar o arquivo inteiro na memória
- Endpoint de healthcheck em `/health`
- Timeouts configuráveis para proteger a instância em produção
- Validação de URL e protocolo
- Limite técnico de tamanho de arquivo configurável por variável de ambiente
- Remoção de credenciais fixas do `docker-compose.yml`

## Variáveis de ambiente

Copie `.env.example` para `.env` e preencha os valores:

- `R2_ENDPOINT`
- `R2_ACCESS_KEY`
- `R2_SECRET_KEY`
- `R2_BUCKET`
- `PUBLIC_URL`
- `PORT`
- `UPLOAD_TIMEOUT_MS`
- `REQUEST_TIMEOUT_MS`
- `MAX_FILE_SIZE_BYTES`

## Rodando com Docker Compose

```bash
cp .env.example .env
# edite o .env

docker compose up --build
```

## Endpoints

### Health

```http
GET /health
```

### Upload

```http
POST /upload-from-url
Content-Type: application/json

{
  "blobUrl": "https://example.com/file.pdf"
}
```

Resposta:

```json
{
  "url": "https://your-public-bucket-url.r2.dev/uuid.pdf",
  "contentType": "application/pdf",
  "bytes": 12345
}
```
