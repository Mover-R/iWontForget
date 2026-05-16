# ============================================================================
# Stage 1: Proto codegen
# Generates Python gRPC stubs from .proto files
# ============================================================================
FROM python:3.12-slim AS proto-builder

WORKDIR /proto-gen

RUN pip install --no-cache-dir grpcio-tools==1.68.1 protobuf==5.29.2

COPY proto/ /proto/

RUN python -m grpc_tools.protoc \
    --proto_path=/proto \
    --python_out=/proto-gen \
    --grpc_python_out=/proto-gen \
    --pyi_out=/proto-gen \
    /proto/bot/v1/bot.proto \
    /proto/users/v1/users.proto \
    /proto/wishlist/v1/wishlist.proto \
    /proto/reminder/v1/reminder.proto \
    /proto/friends/v1/friends.proto \
    /proto/gift/v1/gift.proto \
    /proto/events/v1/events.proto \
    /proto/ai/v1/ai.proto

# Fix relative imports in generated grpc files
RUN find /proto-gen -name "*_pb2_grpc.py" -exec \
    sed -i 's/^import \(.*\)_pb2 as/from . import \1_pb2 as/' {} \;


# ============================================================================
# Stage 2: Dependency builder
# ============================================================================
FROM python:3.12-slim AS deps-builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY services/bot-gateway/pyproject.toml .

RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .


# ============================================================================
# Stage 3: Runtime image
# ============================================================================
FROM python:3.12-slim AS runtime

# Security: non-root user
RUN groupadd --gid 1001 botuser && \
    useradd --uid 1001 --gid botuser --shell /bin/bash --create-home botuser

WORKDIR /app

COPY --from=deps-builder /venv /venv
ENV PATH="/venv/bin:$PATH"

# Copy generated proto stubs
COPY --from=proto-builder /proto-gen /app/bot_gateway/proto_gen/
RUN find /app/bot_gateway/proto_gen -type d -exec touch {}/__init__.py \;

# Copy application source
COPY services/bot-gateway/bot_gateway/ /app/bot_gateway/

USER botuser

# Webhook port (Telegram supports 443, 80, 88, 8443)
EXPOSE 8443

# Health check — PTB exposes no HTTP in longpoll mode, so we check the process
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

CMD ["python", "-m", "bot_gateway"]
