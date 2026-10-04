# Runtime image: CA certificates plus the static binary built by goreleaser.
FROM --platform=$BUILDPLATFORM alpine:3.20 AS certs
RUN apk add --no-cache ca-certificates && update-ca-certificates

FROM scratch
ARG TARGETPLATFORM
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
# dockers_v2 stages the goreleaser-built binary at <os>/<arch>/ and builds with
# --platform, so TARGETPLATFORM selects the right binary per arch.
COPY $TARGETPLATFORM/s3-sites /bin/s3-sites
ENTRYPOINT ["/bin/s3-sites"]
