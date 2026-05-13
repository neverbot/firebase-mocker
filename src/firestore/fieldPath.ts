/**
 * Parser for Firestore `field_path` strings that mirrors the Admin SDK's
 * canonical wire encoding (`@google-cloud/firestore/src/path.ts`).
 *
 * The SDK escapes any segment that does not match `^[_a-zA-Z][_a-zA-Z0-9]*$`
 * by wrapping it in backticks and escaping embedded `\\` and `` ` `` with a
 * backslash. This module reverses that encoding so colons, dashes, spaces,
 * and literal dots inside a segment survive the round trip.
 */

/**
 * Split a field_path string into segments, honoring backtick escaping.
 *
 * @example
 * splitFieldPath('a.b')               // ['a', 'b']
 * splitFieldPath('a.`weird-key`')     // ['a', 'weird-key']
 * splitFieldPath('`a.b`.c')           // ['a.b', 'c']
 * splitFieldPath('`a\\`b`')           // ['a`b']
 * splitFieldPath('`a\\\\b`')          // ['a\\b']
 */
export function splitFieldPath(path: string): string[] {
  if (path === '') {
    return [''];
  }
  const segments: string[] = [];
  let buf = '';
  let i = 0;
  while (i < path.length) {
    const c = path[i];
    if (c === '`') {
      i++;
      while (i < path.length) {
        const cc = path[i];
        if (cc === '\\') {
          const next = path[i + 1];
          if (next === '`' || next === '\\') {
            buf += next;
            i += 2;
            continue;
          }
          buf += cc;
          i++;
        } else if (cc === '`') {
          i++;
          break;
        } else {
          buf += cc;
          i++;
        }
      }
    } else if (c === '.') {
      segments.push(buf);
      buf = '';
      i++;
    } else {
      buf += c;
      i++;
    }
  }
  segments.push(buf);
  return segments;
}
