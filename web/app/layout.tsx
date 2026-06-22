import type { Metadata } from "next";
import "./globals.css";
import { Providers } from "./providers";

export const metadata: Metadata = {
  title: "EduInsight — Income & Math Outcomes",
  description:
    "Exploring the correlation between household income and grade-8 math proficiency across US schools.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>
        <Providers>
          <header className="border-b border-[var(--border)] px-6 py-4 flex items-center gap-3">
            <span className="text-xl font-bold tracking-tight">EduInsight</span>
            <span className="text-[var(--muted)] text-sm">
              Income &amp; Math Outcomes · US Grade-8 Schools · 2020–21
            </span>
          </header>
          <main className="min-h-screen">{children}</main>
          <footer className="border-t border-[var(--border)] px-6 py-4 text-xs text-[var(--muted)]">
            Data: Urban Institute EdFacts + US Census ACS · Income proxy: % households
            earning $150k+ in school ZIP · Math: grade-8 proficiency midpoint ·
            Pearson r computed per county/state (min 3 schools)
          </footer>
        </Providers>
      </body>
    </html>
  );
}
