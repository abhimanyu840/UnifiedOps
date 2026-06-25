import type { SVGProps } from 'react';

type Props = SVGProps<SVGSVGElement> & { size?: number };

/* ---------------------------------------------------------------------------
   Vendor wordmarks rendered inline as SVG, matching the real brand styling
   at small badge sizes. Each badge is a square so the System Health grid
   stays uniform; the wordmark inside is letter-spaced to read clearly.
   --------------------------------------------------------------------------- */

export function HitachiBadge({ size = 40, ...rest }: Props) {
  return (
    <svg width={size} height={size} viewBox="0 0 100 100" fill="none" {...rest}>
      <rect width="100" height="100" rx="10" fill="#FFFFFF" />
      <text
        x="50"
        y="58"
        textAnchor="middle"
        fontFamily="'Inter', 'Helvetica Neue', Arial, sans-serif"
        fontWeight="900"
        fontSize="20"
        letterSpacing="0.5"
        fill="#E60012"
      >
        HITACHI
      </text>
    </svg>
  );
}

export function NetAppBadge({ size = 40, ...rest }: Props) {
  return (
    <svg width={size} height={size} viewBox="0 0 100 100" fill="none" {...rest}>
      <rect width="100" height="100" rx="10" fill="#0067C5" />
      <path d="M20 62 L20 22 L80 22 L80 62 L56 62 L56 40 L44 40 L44 62 Z" fill="#FFFFFF" />
      <text
        x="50"
        y="86"
        textAnchor="middle"
        fontFamily="'Inter', 'Helvetica Neue', Arial, sans-serif"
        fontWeight="800"
        fontSize="13"
        fill="#FFFFFF"
      >
        NetApp™
      </text>
    </svg>
  );
}

export function DellBadge({ size = 40, ...rest }: Props) {
  return (
    <svg width={size} height={size} viewBox="0 0 100 100" fill="none" {...rest}>
      <rect width="100" height="100" rx="10" fill="#FFFFFF" />
      <image href="/dell-logo.png" x="5" y="5" width="90" height="90" />
    </svg>
  );
}

export function BrocadeBadge({ size = 40, ...rest }: Props) {
  return (
    <svg width={size} height={size} viewBox="0 0 100 100" fill="none" {...rest}>
      <rect width="100" height="100" rx="10" fill="#FFFFFF" />
      {/* Stylised "B" mark (two loops + slanted tail) */}
      <path
        d="M28 22
           L60 22
           C72 22 78 30 78 38
           C78 44 74 48 70 50
           C76 52 80 56 80 64
           C80 72 74 78 62 78
           L28 78
           L28 22
           Z
           M40 35
           L40 45
           L58 45
           C62 45 64 43 64 40
           C64 37 62 35 58 35
           Z
           M40 56
           L40 66
           L60 66
           C64 66 66 64 66 61
           C66 58 64 56 60 56
           Z"
        fill="#FF1100"
      />
    </svg>
  );
}

export function TotalBadge({ size = 40, ...rest }: Props) {
  return (
    <svg width={size} height={size} viewBox="0 0 100 100" fill="none" {...rest}>
      <defs>
        <linearGradient id="tot-grad" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="#475569" />
          <stop offset="100%" stopColor="#1F2937" />
        </linearGradient>
      </defs>
      <rect width="100" height="100" rx="10" fill="url(#tot-grad)" />
      <rect x="22" y="28"  width="56" height="8" rx="2" fill="#E5E7EB" />
      <rect x="22" y="46"  width="56" height="8" rx="2" fill="#E5E7EB" />
      <rect x="22" y="64"  width="56" height="8" rx="2" fill="#E5E7EB" />
      <circle cx="28" cy="32" r="2.5" fill="#00D26A" />
      <circle cx="28" cy="50" r="2.5" fill="#00D26A" />
      <circle cx="28" cy="68" r="2.5" fill="#00D26A" />
    </svg>
  );
}
