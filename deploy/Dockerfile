# Build context is the repository root: docker build -f deploy/Dockerfile .
FROM golang:1.27 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/coordinator ./cmd/storagenode \
    && mkdir /out/data

# /data is created in the image owned by nonroot so a fresh named volume mounted
# there inherits that ownership; distroless has no shell or chown to fix it later.
FROM gcr.io/distroless/static:nonroot
COPY --from=builder /out/coordinator /out/storagenode /
COPY --from=builder --chown=nonroot:nonroot /out/data /data
ENV CAIRN_DATA_DIR=/data
USER nonroot
