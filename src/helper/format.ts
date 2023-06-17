
/**
 * Format a filesize into a friendly string.
 */
export const fileSize = (raw: number | { size: number }) => {
  let size: number;
  if (typeof raw === 'number') {
    size = raw;
  } else {
    size = raw.size;
  }

  if (size > 1024 * 1024) {
    return (size / (1024 * 1024)).toFixed(2) + 'mb';
  } else if (size > 1024) {
    return (size / 1024).toFixed(2) + 'kb';
  } else {
    return size + 'b';
  }
};
