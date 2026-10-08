# Application image for the validator, AI repair worker, downstream consumer
# and the one-shot connector registration job.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
ARG EXTRA_REQUIREMENTS=""
COPY requirements*.txt ./
RUN pip install -r requirements.txt && if [ -n "$EXTRA_REQUIREMENTS" ]; then pip install -r "$EXTRA_REQUIREMENTS"; fi

# Non-root user (also makes RLIMIT_NPROC effective inside the sandbox child).
RUN useradd --create-home --uid 10001 app && mkdir -p /app/state /app/lake && chown app:app /app/state /app/lake
COPY --chown=app:app src ./src
COPY --chown=app:app config ./config
USER app

CMD ["python", "-m", "src.validator.consumer"]
