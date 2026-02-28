import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./app/**/*.{ts,tsx}", "./components/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "#0b1220",
        panel: "#111b2e",
        line: "#22304b",
        text: "#e5edf9",
        muted: "#93a4c3"
      }
    }
  },
  plugins: []
};

export default config;
