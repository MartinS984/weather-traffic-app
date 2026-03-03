# Use the lightweight Alpine Node image
FROM node:20-alpine

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy dependency definitions and install them
COPY package*.json ./
RUN npm install --omit=dev

# Copy the application code
COPY index.js .

# Start the producer
CMD [ "node", "index.js" ]