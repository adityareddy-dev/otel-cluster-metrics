# Image for the live copy. Build context is examples/grafana (see compose.yaml).
FROM node:24-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund && mkdir /state && chown node:node /state
COPY app.cjs load.cjs ./
USER node
CMD ["node", "app.cjs"]
