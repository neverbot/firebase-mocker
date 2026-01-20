# Local Installation of firebase-mocker

This guide explains how to install `firebase-mocker` as a local dependency in another project without publishing it to npm.

## Option 1: npm link (Recommended for development)

This is the best option when you are actively developing both projects.

### Step 1: In the firebase-mocker project

```bash
cd /Users/ivan/Projects/neverbot/firebase-mocker/firebase-mocker
npm run build  # Make sure to compile TypeScript first
npm link
```

This creates a global symbolic link that points to your local project.

### Step 2: In your test project

```bash
cd /path/to/your/test-project
npm link firebase-mocker
```

Now `firebase-mocker` will be available as if it were a normal dependency.

### To unlink

```bash
# In your test project
npm unlink firebase-mocker

# In firebase-mocker (optional, to clean up the global link)
cd /Users/ivan/Projects/neverbot/firebase-mocker/firebase-mocker
npm unlink
```

**Advantages:**
- Changes in `firebase-mocker` are automatically reflected
- You don't need to reinstall when making changes
- Works like a normal dependency

**Disadvantages:**
- Requires running `npm run build` every time you change the code
- Links can break if you move folders

---

## Option 2: file: protocol (Recommended for CI/CD)

This option uses a relative or absolute path in the `package.json`.

### In your test project, edit `package.json`:

```json
{
  "dependencies": {
    "firebase-mocker": "file:../firebase-mocker/firebase-mocker"
  }
}
```

Or with absolute path:

```json
{
  "dependencies": {
    "firebase-mocker": "file:/Users/ivan/Projects/neverbot/firebase-mocker/firebase-mocker"
  }
}
```

Then run:

```bash
npm install
```

**Advantages:**
- Works well in CI/CD if paths are consistent
- No additional steps required after the first installation
- Can be versioned in git (if using relative path)

**Disadvantages:**
- You need to reinstall when you change firebase-mocker code
- Absolute paths don't work well on different machines

---

## Option 3: Direct path in npm install

You can install directly from the path without modifying `package.json`:

```bash
cd /path/to/your/test-project
npm install /Users/ivan/Projects/neverbot/firebase-mocker/firebase-mocker
```

This will automatically add the dependency to your `package.json` with the `file:` protocol.

---

## Recommendation

For active development: **Use `npm link`**
- More flexible during development
- Changes are automatically reflected

For stable use or CI/CD: **Use `file:` protocol**
- More predictable
- Better for CI/CD environments

---

## Important note about TypeScript

Make sure `firebase-mocker` is compiled before using it:

```bash
cd /Users/ivan/Projects/neverbot/firebase-mocker/firebase-mocker
npm run build
```

If you use `npm link`, you can create a watch script to automatically recompile:

```bash
# In firebase-mocker/package.json, add:
"scripts": {
  "watch": "tsc --watch"
}
```

Then run `npm run watch` in a terminal while developing.
