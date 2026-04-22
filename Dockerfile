FROM node:18

WORKDIR /app

# install client API.co.id (contoh generic, sesuaikan jika mereka kasih package)
RUN npm init -y

# install express untuk expose endpoint
RUN npm install express axios

COPY server.js .

EXPOSE 3003

CMD ["node", "server.js"]
