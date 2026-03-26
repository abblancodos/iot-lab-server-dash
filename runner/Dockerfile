FROM ubuntu:22.04

ARG RUNNER_VERSION=2.314.1

ENV DEBIAN_FRONTEND=noninteractive

# Dependencias base
RUN apt-get update && apt-get install -y \
    curl \
    git \
    sudo \
    build-essential \
    libssl-dev \
    pkg-config \
    docker.io \
    docker-compose-plugin \
    rsync \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Usuario runner
RUN useradd -m -s /bin/bash runner && \
    usermod -aG sudo runner && \
    usermod -aG docker runner && \
    echo "runner ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

USER runner
WORKDIR /home/runner

# Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/home/runner/.cargo/bin:${PATH}"

# sqlx-cli
RUN cargo install sqlx-cli --no-default-features --features postgres

# Node via nvm
ENV NVM_DIR="/home/runner/.nvm"
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash && \
    . "$NVM_DIR/nvm.sh" && \
    nvm install 20 && \
    nvm alias default 20

ENV PATH="$NVM_DIR/versions/node/v20/bin:${PATH}"

# GitHub Actions runner
RUN mkdir actions-runner && cd actions-runner && \
    curl -o runner.tar.gz -L \
    https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz && \
    tar xzf runner.tar.gz && \
    rm runner.tar.gz

WORKDIR /home/runner/actions-runner

COPY --chown=runner:runner entrypoint.sh /home/runner/entrypoint.sh
RUN chmod +x /home/runner/entrypoint.sh

ENTRYPOINT ["/home/runner/entrypoint.sh"]