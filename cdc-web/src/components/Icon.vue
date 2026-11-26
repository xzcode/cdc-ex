<script setup lang="ts">
withDefaults(defineProps<{ name: string; size?: number }>(), { size: 20 })
const paths: Record<string, string[]> = {
  grid: ['M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z'],
  flow: ['M4 4h6v6H4z M14 14h6v6h-6z', 'M10 7h5a2 2 0 0 1 2 2v5', 'm14 11 3 3 3-3'],
  database: ['M20 6c0 2.2-3.6 4-8 4S4 8.2 4 6s3.6-4 8-4 8 1.8 8 4Z', 'M4 6v12c0 2.2 3.6 4 8 4s8-1.8 8-4V6', 'M4 12c0 2.2 3.6 4 8 4s8-1.8 8-4'],
  audit: ['M8 3H5a2 2 0 0 0-2 2v15a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V5a2 2 0 0 0-2-2h-3', 'M8 2h8v4H8z', 'M7 11h10 M7 15h10 M7 19h6'],
  plus: ['M12 5v14 M5 12h14'], arrow: ['M4 12h16 m-6-6 6 6-6 6'],
  chevron: ['m9 5 7 7-7 7'], close: ['m6 6 12 12 M18 6 6 18'],
  check: ['m5 12 4 4L19 6'], shield: ['M12 3 3 7v5c0 5 9 10 9 10s9-5 9-10V7l-9-4Z', 'm8 12 3 3 5-6'],
  refresh: ['M20 7a9 9 0 0 0-15-2L2 8', 'M2 2v6h6', 'M4 17a9 9 0 0 0 15 2l3-3', 'M22 22v-6h-6'],
  play: ['m8 4 12 8-12 8V4Z'], pause: ['M7 4v16 M17 4v16'], stop: ['M5 5h14v14H5z'],
  search: ['M10 18a8 8 0 1 0 0-16 8 8 0 0 0 0 16Z', 'm16 16 6 6'],
  clock: ['M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20Z', 'M12 6v6l4 2'],
  bolt: ['m14 2-9 12h7l-2 8 9-12h-7l2-8Z'],
  lock: ['M5 10h14v11H5z', 'M8 10V6a4 4 0 0 1 8 0v4', 'M12 14v3'],
  external: ['M14 3h7v7', 'M21 3 10 14', 'M10 3H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-5'],
  info: ['M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20Z', 'M12 11v6 M12 7h.01'],
  warning: ['M12 3 1 21h22L12 3Z', 'M12 9v5 M12 17h.01'],
  copy: ['M8 8h13v13H8z', 'M16 8V3H3v13h5'],
  edit: ['m15 4 5 5 M4 16l-1 5 5-1L21 7l-5-5L4 16Z'],
  logout: ['M9 3H3v18h6', 'M9 12h13 m-5-5 5 5-5 5'],
  compare: ['M8 3 3 8l5 5 M3 8h16', 'm16 11 5 5-5 5 M21 16H5'],
  code: ['m8 5-7 7 7 7 m8-14 7 7-7 7 M14 3l-4 18'],
  globe: ['M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20Z', 'M2 12h20 M12 2c6 6 6 14 0 20-6-6-6-14 0-20Z'],
  menu: ['M3 6h18 M3 12h18 M3 18h18'],
  empty: ['m3 9 4-6h10l4 6v12H3V9Z', 'M3 9h5l2 4h4l2-4h5'],
}
</script>
<template><svg :width="size" :height="size" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path v-for="(path, index) in (paths[name] || paths.info)" :key="index" :d="path" /></svg></template>
