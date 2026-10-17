FROM node:22-alpine
WORKDIR /app
# Les dates sont traitées en UTC (mssql lit les datetime SQL Server en UTC)
ENV NODE_ENV=production TZ=UTC
COPY package*.json ./
RUN npm ci --omit=dev
COPY --chown=node:node . .
USER node
EXPOSE 3001
HEALTHCHECK --interval=60s --timeout=5s --start-period=20s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:3001/healthz').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
CMD ["node", "index.js"]
