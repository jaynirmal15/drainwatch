# The probe image. Static binary on scratch: no shell, no package manager, and
# nothing in the image that could answer a port other than the probe itself.
FROM golang:1.23-alpine AS build

WORKDIR /src

# Dependencies first, so that source edits do not invalidate the module layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=0.1.0
ARG GIT_COMMIT=unknown

# CGO off and netgo/osusergo so the binary has no dynamic loader dependency and
# can run on scratch. Trimpath keeps build paths out of the binary so that two
# builds of the same commit are byte-identical.
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -tags netgo,osusergo \
      -ldflags "-s -w \
        -X github.com/jaynirmal15/drainwatch/internal/report.Version=${VERSION} \
        -X github.com/jaynirmal15/drainwatch/internal/report.GitCommit=${GIT_COMMIT}" \
      -o /out/drainwatch ./cmd/drainwatch

FROM scratch
COPY --from=build /out/drainwatch /drainwatch
# Numeric UID: scratch has no /etc/passwd, and the manifest sets runAsNonRoot.
USER 65532:65532
EXPOSE 7001/tcp
EXPOSE 7002/udp
EXPOSE 7003/tcp
ENTRYPOINT ["/drainwatch", "probe"]
