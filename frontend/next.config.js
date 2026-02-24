/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
  env: {
    NEXT_PUBLIC_API_URL: "https://dina-backend-848917307732.southamerica-east1.run.app",
  },
};

module.exports = nextConfig;
