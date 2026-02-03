import eslint from '@eslint/js';
import globals from 'globals';
import tseslint from 'typescript-eslint';
import prettier from 'eslint-plugin-prettier';
import pluginImport from 'eslint-plugin-import';

export default tseslint.config(
  {
    ignores: [
      'eslint.config.mjs', 
      'dist/**', 
      'node_modules/**', 
      'coverage/**', 
      'dist-test/**', 
      'data/**', 
      'docs/**', 
      'assets/**',
      'scripts/**',
      '*.log',
      'server.log',
      'package-lock.json',
      'tsconfig.build.json',
      'tsconfig.scripts.json',
      'nest-cli.json',
      'Dockerfile',
      'readme.md'
    ],
  },
  // TypeScript configuration for source files
  ...tseslint.configs.recommendedTypeChecked.map(config => ({
    ...config,
    files: ['src/**/*.ts', 'test/**/*.ts'],
    rules: {
      ...config.rules,
      '@typescript-eslint/no-restricted-imports': 'off',
    },
  })),
  ...tseslint.configs.stylisticTypeChecked.map(config => ({
    ...config,
    files: ['src/**/*.ts', 'test/**/*.ts'],
    rules: {
      ...config.rules,
      '@typescript-eslint/no-restricted-imports': 'off',
    },
  })),
  {
    files: ['src/**/*.ts'],
    languageOptions: {
      globals: {
        ...globals.node,
      },
      sourceType: 'module',
      parserOptions: {
        project: './tsconfig.json',
        tsconfigRootDir: import.meta.dirname,
        ecmaVersion: 2023,
      },
    },
    plugins: {
      'prettier': prettier,
      import: pluginImport,
    },
    settings: {
      'import/resolver': {
        typescript: {
          alwaysTryTypes: true,
          project: './tsconfig.json',
        },
        node: {
          extensions: ['.js', '.jsx', '.ts', '.tsx'],
          // Allow .js extension to resolve to .ts files (TypeScript standard)
          tryExtensions: ['.ts', '.tsx', '.js', '.jsx'],
        },
      },
      'import/extensions': ['.js', '.jsx', '.ts', '.tsx'],
    },
    rules: {
      // Import plugin rules
      'import/no-unresolved': 'error',
      'import/order': [
        'warn',
        {
          groups: [
            'builtin',
            'external',
            'internal',
            'parent',
            'sibling',
            'index',
          ],
          'newlines-between': 'never',
          alphabetize: {
            order: 'asc',
            caseInsensitive: true,
          },
        },
      ],
      'import/no-duplicates': 'warn',
      'import/no-unused-modules': 'warn',
      
      // TypeScript-specific rules
      '@typescript-eslint/no-restricted-imports': 'off',
      '@typescript-eslint/explicit-function-return-type': 'off',
      '@typescript-eslint/explicit-member-accessibility': 'off',
      '@typescript-eslint/explicit-module-boundary-types': 'off',
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/no-unsafe-argument': 'off',
      '@typescript-eslint/no-unsafe-assignment': 'off',
      '@typescript-eslint/no-unsafe-call': 'off',
      '@typescript-eslint/no-unsafe-member-access': 'off',
      '@typescript-eslint/no-unsafe-return': 'off',
      '@typescript-eslint/prefer-nullish-coalescing': 'off',
      '@typescript-eslint/naming-convention': 'off',
      '@typescript-eslint/no-empty-function': 'off',
      '@typescript-eslint/require-await': 'off',
      '@typescript-eslint/no-unused-vars': ['warn', {
        argsIgnorePattern: '^_',
        varsIgnorePattern: '^_'
      }],
      '@typescript-eslint/prefer-optional-chain': 'off',
      '@typescript-eslint/prefer-readonly': 'warn',
      '@typescript-eslint/promise-function-async': 'warn',
      '@typescript-eslint/no-require-imports': 'off',
      '@typescript-eslint/no-useless-constructor': 'off',
      '@typescript-eslint/no-floating-promises': 'off',
      '@typescript-eslint/dot-notation': 'off',
      'max-lines-per-function': 'off',
      'max-len': 'off',
      'complexity': 'off',
      'no-console': 'warn',
      'prefer-const': 'warn',
      'no-var': 'warn',
      'eqeqeq': 'warn',
      'curly': 'warn',
      'arrow-body-style': 'off',
      'sort-imports': 'off',
      'no-useless-constructor': 'off',
      'new-cap': 'off',

      // Prettier rule last so it doesn't override ESLint rules
      'prettier/prettier': [
        'error',
        {},
        {
          usePrettierrc: true,
        },
      ],
    },
  },
  {
    files: ['test/**/*.ts'],
    languageOptions: {
      globals: {
        ...globals.node,
        ...globals.mocha,
      },
      sourceType: 'module',
      parserOptions: {
        project: './test/tsconfig.json',
        tsconfigRootDir: import.meta.dirname,
        ecmaVersion: 2023,
      },
    },
    plugins: {
      'prettier': prettier,
      import: pluginImport,
    },
    settings: {
      'import/resolver': {
        typescript: {
          alwaysTryTypes: true,
          project: './test/tsconfig.json',
        },
        node: {
          extensions: ['.js', '.jsx', '.ts', '.tsx'],
          // Allow .js extension to resolve to .ts files (TypeScript standard)
          tryExtensions: ['.ts', '.tsx', '.js', '.jsx'],
        },
      },
      'import/extensions': ['.js', '.jsx', '.ts', '.tsx'],
    },
    rules: {
      // Import plugin rules
      'import/no-unresolved': 'error',
      'import/order': [
        'warn',
        {
          groups: [
            'builtin',
            'external',
            'internal',
            'parent',
            'sibling',
            'index',
          ],
          'newlines-between': 'never',
          alphabetize: {
            order: 'asc',
            caseInsensitive: true,
          },
        },
      ],
      'import/no-duplicates': 'warn',
      'import/no-unused-modules': 'warn',
      
      // TypeScript-specific rules      
      '@typescript-eslint/explicit-function-return-type': 'off',
      '@typescript-eslint/explicit-member-accessibility': 'off',
      '@typescript-eslint/explicit-module-boundary-types': 'off',
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/no-unsafe-argument': 'off',
      '@typescript-eslint/no-unsafe-assignment': 'off',
      '@typescript-eslint/no-unsafe-call': 'off',
      '@typescript-eslint/no-unsafe-member-access': 'off',
      '@typescript-eslint/no-unsafe-return': 'off',
      '@typescript-eslint/prefer-nullish-coalescing': 'off',
      '@typescript-eslint/naming-convention': 'off',
      '@typescript-eslint/no-empty-function': 'off',
      '@typescript-eslint/require-await': 'off',
      '@typescript-eslint/no-unused-vars': ['warn', {
        argsIgnorePattern: '^_',
        varsIgnorePattern: '^_'
      }],
      '@typescript-eslint/no-floating-promises': 'off',
      '@typescript-eslint/prefer-optional-chain': 'off',
      '@typescript-eslint/prefer-readonly': 'warn',
      '@typescript-eslint/promise-function-async': 'warn',
      '@typescript-eslint/no-require-imports': 'off',
      '@typescript-eslint/no-useless-constructor': 'off',
      '@typescript-eslint/dot-notation': 'off',
      '@typescript-eslint/no-unused-expressions': 'off', // Allow unused expressions in Mocha tests
      'max-lines-per-function': 'off',
      'max-len': 'off',
      'complexity': 'off',
      'no-console': 'off', // Allow console in tests
      'prefer-const': 'warn',
      'no-var': 'warn',
      'eqeqeq': 'warn',
      'curly': 'warn',
      'arrow-body-style': 'off',
      'sort-imports': 'off',
      'no-useless-constructor': 'off',
      'new-cap': 'off',

      // Prettier rule last so it doesn't override ESLint rules
      'prettier/prettier': [
        'error',
        {},
        {
          usePrettierrc: true,
        },
      ],
    },
  },
);
