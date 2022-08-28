import { babel } from '@rollup/plugin-babel';
import { nodeResolve } from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';

export default [
    {
        input: "index.js",
        output: {
            file: "dist/main.js",
            format: "iife",
        },
        plugins: [
            nodeResolve({ browser: true }),
            commonjs(),
            babel({
                exclude: 'node_modules/**',
                babelHelpers: 'bundled'
            })
        ]
    }
];