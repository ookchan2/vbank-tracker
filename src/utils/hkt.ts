/**
 * HKT (Hong Kong Time, UTC+8) timezone utilities.
 * Uses Intl.DateTimeFormat for proper timezone handling.
 * @author Alfie
 */

/** Get today's date string in HKT (YYYY-MM-DD) */
export function hktToday(): string {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Hong_Kong',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(new Date());
}

/** Get date string N days ago in HKT (YYYY-MM-DD) */
export function hktNDaysAgo(n: number): string {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Hong_Kong',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(d);
}

/** Get current HKT datetime string (YYYY-MM-DD HH:MM) */
export function hktNow(): string {
  const d = new Date();
  const date = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Hong_Kong',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(d);
  const time = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Hong_Kong',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(d);
  return `${date} ${time}`;
}
