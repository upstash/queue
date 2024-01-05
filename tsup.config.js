import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["cjs", "esm"],
  sourcemap: false,
  clean: true,
  bundle: true,
  dts: true,
  minify: true,
  treeshake: true,
});
