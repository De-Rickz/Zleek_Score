import type { Metadata } from "next";
import { IBM_Plex_Mono, IBM_Plex_Sans } from "next/font/google";
import "./globals.css";

// IBM Plex Sans for text, IBM Plex Mono for numbers (CLAUDE.md design rules).
const plexSans = IBM_Plex_Sans({
  variable: "--font-plex-sans",
  subsets: ["latin"],
  weight: ["400", "600"],
});

const plexMono = IBM_Plex_Mono({
  variable: "--font-plex-mono",
  subsets: ["latin"],
  weight: ["400", "600"],
});

export const metadata: Metadata = {
  title: { default: "Zleek Score", template: "%s · Zleek Score" },
  description: "Polymarket markets ranked by tremor score, a 0-100 measure of unusual price movement.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  // The font variables go on <html>: Tailwind sets font-family on html, so a
  // variable defined only on <body> never reaches it.
  return (
    <html lang="en" className={`${plexSans.variable} ${plexMono.variable}`}>
      <body className="antialiased">
        {children}
      </body>
    </html>
  );
}
