# Build stage
FROM node:18-alpine AS builder

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy source files
COPY . .

# Build the documentation
RUN npm run build

# Production stage
FROM nginx:alpine

# Copy built documentation from builder stage
COPY --from=builder /app/build /usr/share/nginx/html

# Copy nginx configuration if needed
COPY nginx.conf /etc/nginx/conf.d/default.conf 2>/dev/null || :

# Expose port 80
EXPOSE 80

# Start nginx
CMD ["nginx", "-g", "daemon off;"]