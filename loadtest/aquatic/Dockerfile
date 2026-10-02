# Linux image for loadtest/aquatic.sh: aquatic_ws (pinned revision) and this workspace's
# wt-tracker + wt-loadgen, built from source. Build context: the repository root.
FROM rust:1-trixie AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake && rm -rf /var/lib/apt/lists/*
ARG AQUATIC_REV
RUN git clone https://github.com/greatest-ape/aquatic /aquatic \
 && cd /aquatic && git checkout "$AQUATIC_REV" \
 && cargo build --release -p aquatic_ws
COPY . /wt
RUN cd /wt && cargo build --release -p wt-server -p wt-loadgen

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl procps ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /aquatic/target/release/aquatic_ws /wt/target/release/wt-tracker \
     /wt/target/release/wt-loadgen /usr/local/bin/
COPY loadtest/aquatic/run.sh /run.sh
ENTRYPOINT ["bash", "/run.sh"]
