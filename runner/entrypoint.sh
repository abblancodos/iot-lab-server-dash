#!/bin/bash
set -e

# Configurar el runner si no está configurado aún
if [ ! -f ".runner" ]; then
    ./config.sh \
        --url "${GITHUB_RUNNER_URL}" \
        --token "${GITHUB_RUNNER_TOKEN}" \
        --name "${GITHUB_RUNNER_NAME:-iot-backupserver}" \
        --labels "${GITHUB_RUNNER_LABELS:-self-hosted,linux,iot-backupserver}" \
        --unattended \
        --replace
fi

# Iniciar el runner
exec ./run.sh
