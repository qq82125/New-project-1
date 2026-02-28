/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  async rewrites() {
    const adminBase = (process.env.ADMIN_API_INTERNAL_BASE || "http://admin-api:8789").replace(/\/+$/, "");
    return [
      {
        source: "/api/:path*",
        destination: `${adminBase}/api/:path*`,
      },
      {
        source: "/admin/:path*",
        destination: `${adminBase}/admin/:path*`,
      },
      {
        source: "/admin",
        destination: `${adminBase}/admin`,
      },
    ];
  },
};

export default nextConfig;
