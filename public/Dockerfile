# Use an official lightweight Node.js image
FROM node:18-slim

# Install FFmpeg inside the hosting container
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# Create and set the working directory
WORKDIR /usr/src/app

# Copy dependency files and install them
COPY package*.json ./
RUN npm install --production

# Copy the rest of your application code (server.js, html files, etc.)
COPY . .

# Expose the port your server runs on
EXPOSE 3000

# Start the application
CMD ["node", "server.js"]