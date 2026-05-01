import html from "eslint-plugin-html";
import js from "@eslint/js";

export default [
    js.configs.recommended,
    {
        files: ["**/*.html"],
        plugins: {
            html
        },
        languageOptions: {
            globals: {
                L: "readonly",
                d3: "readonly",
                google: "readonly",
                gtag: "readonly",
                window: "readonly",
                document: "readonly",
                localStorage: "readonly",
                location: "readonly",
                history: "readonly",
                URLSearchParams: "readonly",
                URL: "readonly",
                fetch: "readonly",
                console: "readonly",
                setTimeout: "readonly",
                clearTimeout: "readonly"
            }
        },
        rules: {
            "no-unused-vars": "warn",
            "no-undef": "warn"
        }
    }
];
