from __future__ import annotations

import asyncio
import os
import queue
import random
import threading
import webbrowser
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

# Allow running as a script: `python fintech_news_scraper/gui_app.py`
# by ensuring the project root is on sys.path before importing package modules.
if __name__ == "__main__" and (__package__ is None or __package__ == ""):
    try:
        import sys

        project_root = Path(__file__).resolve().parents[1]
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
    except Exception:
        pass

import pandas as pd
import tkinter as tk
import tkinter.font as tkfont
from tkinter import ttk

from fintech_news_scraper.breaking import is_breaking
from fintech_news_scraper.config import load_config
from fintech_news_scraper.pipeline import run_pipeline
from fintech_news_scraper.saved_store import load_saved, remove_saved, save_article
from fintech_news_scraper.types import Article

def _safe_str(x: Any) -> str:
    if x is None:
        return ""
    return str(x)


def _parse_dt(s: Any) -> datetime | None:
    try:
        return pd.to_datetime(s, utc=True, errors="coerce").to_pydatetime()
    except Exception:
        return None


class ScrollFrame(ttk.Frame):
    def __init__(self, master: tk.Misc, *, max_content_width: int | None = 980):
        super().__init__(master)
        self._max_content_width = max_content_width
        self.canvas = tk.Canvas(self, highlightthickness=0)
        self.scrollbar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.inner = ttk.Frame(self.canvas, style="App.TFrame")

        self.inner.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")),
        )

        # Center the content within the scrollable area and keep it responsive.
        self._window_id = self.canvas.create_window((0, 0), window=self.inner, anchor="n")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.canvas.bind("<Configure>", self._on_canvas_configure)
        self.canvas.bind("<Map>", self._on_map)

        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")

        # mouse wheel support (Windows) – bind to this canvas only
        self.canvas.bind("<MouseWheel>", self._on_mousewheel)
        self.canvas.bind("<Enter>", lambda _e: self.canvas.focus_set())

        # Apply initial centering once geometry is known.
        self.after(0, self._apply_layout)

    def _on_map(self, _event: tk.Event) -> None:
        # When a tab becomes visible, Tk may not trigger a size change.
        self.after(0, self._apply_layout)

    def _apply_layout(self) -> None:
        try:
            w = int(self.canvas.winfo_width() or 0)
            if w <= 1:
                # Not laid out yet; try again shortly.
                self.after(50, self._apply_layout)
                return
            dummy = type("_E", (), {"width": w})
            self._on_canvas_configure(dummy)  # type: ignore[arg-type]
        except Exception:
            return

    def _on_canvas_configure(self, event: tk.Event) -> None:
        try:
            w = int(getattr(event, "width", 0) or self.canvas.winfo_width() or 0)
            if w <= 0:
                return
            target_w = w
            if self._max_content_width is not None:
                try:
                    target_w = min(w, int(self._max_content_width))
                except Exception:
                    target_w = w
            self.canvas.coords(self._window_id, w // 2, 0)
            self.canvas.itemconfigure(self._window_id, width=target_w)
        except Exception:
            return

    def _on_mousewheel(self, event: tk.Event):
        try:
            self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        except Exception:
            return


class RoundedContainer(tk.Canvas):
    def __init__(
        self,
        master: tk.Misc,
        *,
        radius: int = 12,
        bg: str = "#f6f8fb",
        fill: str = "#ffffff",
        outline: str = "#e2e8f0",
        padding: tuple[int, int] = (12, 10),
        inner_style: str = "Card.TFrame",
    ):
        super().__init__(master, highlightthickness=0, bd=0, background=bg)
        self._radius = int(radius)
        self._bg = bg
        self._fill = fill
        self._outline = outline
        self._padx = int(padding[0])
        self._pady = int(padding[1])

        # A ttk frame is placed on top of the rounded rect.
        self.inner = ttk.Frame(self, style=inner_style)
        self._inner_window_id = self.create_window(
            (self._padx, self._pady),
            window=self.inner,
            anchor="nw",
        )
        self._shape_id = None

        self._height_sync_scheduled = False
        self._last_height: int | None = None

        self.bind("<Configure>", self._redraw)
        self.inner.bind("<Configure>", self._sync_height)
        self.after(0, self._sync_height)

    def _sync_height(self, _event: tk.Event | None = None) -> None:
        # Debounce: Tk can fire a cascade of <Configure> events; doing layout
        # work synchronously here can cause re-entrant recursion.
        if self._height_sync_scheduled:
            return
        self._height_sync_scheduled = True
        self.after_idle(self._apply_height_sync)

    def _apply_height_sync(self) -> None:
        self._height_sync_scheduled = False
        try:
            h = int(self.inner.winfo_reqheight() or 0) + (2 * self._pady)
            # Avoid a 1px canvas during first layout.
            h = max(10, h)
            if self._last_height != h:
                self._last_height = h
                self.configure(height=h)
            self._redraw()
        except Exception:
            return

    def _redraw(self, _event: tk.Event | None = None) -> None:
        try:
            w = int(self.winfo_width() or 0)
            h = int(self.winfo_height() or 0)
            if w <= 2 or h <= 2:
                return

            r = max(2, min(self._radius, (min(w, h) // 2) - 1))

            x1, y1 = 1, 1
            x2, y2 = w - 1, h - 1

            # Keep inner content inside padding.
            inner_w = max(1, w - (2 * self._padx))
            self.coords(self._inner_window_id, self._padx, self._pady)
            self.itemconfigure(self._inner_window_id, width=inner_w)

            points = [
                x1 + r,
                y1,
                x2 - r,
                y1,
                x2,
                y1,
                x2,
                y1 + r,
                x2,
                y2 - r,
                x2,
                y2,
                x2 - r,
                y2,
                x1 + r,
                y2,
                x1,
                y2,
                x1,
                y2 - r,
                x1,
                y1 + r,
                x1,
                y1,
            ]

            if self._shape_id is None:
                self._shape_id = self.create_polygon(
                    points,
                    smooth=True,
                    splinesteps=36,
                    fill=self._fill,
                    outline=self._outline,
                    width=1,
                )
                self.tag_lower(self._shape_id)
            else:
                self.coords(self._shape_id, *points)
                self.itemconfigure(self._shape_id, fill=self._fill, outline=self._outline)
                self.tag_lower(self._shape_id)
        except Exception:
            return

class NewsApp:
    def __init__(self, root: tk.Tk, *, config_path: str, sources_path: str):
        self.root = root
        self.root.title("Financial News Scraper")
        self.root.geometry("1100x700")

        self.config_path = config_path
        self.sources_path = sources_path
        self.cfg = load_config(config_path)
        self.output_dir = Path(self.cfg.raw["storage"]["output_dir"])

        # Live data is always in-memory for display. If Auto-save is enabled,
        # fetched articles are additionally persisted to per-source CSVs.
        self.live_articles: list[Article] = []
        self._live_by_url: dict[str, Article] = {}
        self._seen_urls: set[str] = set()
        self._recent_texts: list[str] = []
        self._recent_urls: list[str] = []

        # Breaking tab data is also in-memory only.
        self.breaking_articles: list[Article] = []
        self._breaking_by_url: dict[str, Article] = {}
        self._breaking_dirty: bool = False

        self._q: queue.Queue[list[Article]] = queue.Queue()
        self._stop = threading.Event()
        self._lock = threading.Lock()

        self.status = tk.StringVar(value="Ready")

        gui_cfg = (self.cfg.raw.get("gui", {}) or {})
        self.auto_save_csv = tk.BooleanVar(value=bool(gui_cfg.get("auto_save_csv", False)))
        self._settings_summary = tk.StringVar(value="")

        # Theme colors are configured in run_gui(). Keep a few handy.
        self._app_bg = "#f6f8fb"
        self._card_bg = "#ffffff"

        self.notebook = ttk.Notebook(root, style="App.TNotebook")
        self.tab_live = ttk.Frame(self.notebook, style="App.TFrame")
        self.tab_breaking = ttk.Frame(self.notebook, style="App.TFrame")
        self.tab_saved = ttk.Frame(self.notebook, style="App.TFrame")
        self.notebook.add(self.tab_live, text="Live")
        self.notebook.add(self.tab_breaking, text="Breaking")
        self.notebook.add(self.tab_saved, text="Saved")
        self.notebook.pack(fill="both", expand=True)

        self._build_live()
        self._build_breaking()
        self._build_saved()

        self._refresh_settings_summary()

        status_bar_outer = RoundedContainer(
            root,
            radius=12,
            bg=self._app_bg,
            fill=self._card_bg,
            outline="#e2e8f0",
            padding=(12, 8),
            inner_style="StatusCard.TFrame",
        )
        status_bar = status_bar_outer.inner
        ttk.Label(status_bar, textvariable=self.status, style="StatusCard.TLabel").pack(side="left")
        status_bar_outer.pack(fill="x", side="bottom", padx=12, pady=(0, 12))

        self._start_live_loop()
        self.refresh_live()
        self.refresh_saved()

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _open_output_folder(self) -> None:
        try:
            p = self.output_dir
            p.mkdir(parents=True, exist_ok=True)
            os.startfile(str(p.resolve()))
        except Exception:
            return

    def _on_autosave_toggle(self) -> None:
        self._refresh_settings_summary()
        if self.auto_save_csv.get():
            self.status.set("Auto-save: ON (per-source CSVs)")
        else:
            self.status.set("Auto-save: OFF")

    def _refresh_settings_summary(self) -> None:
        try:
            raw = self.cfg.raw
            conc = raw.get("concurrency", {}) or {}
            rl = raw.get("rate_limit", {}) or {}
            dd = raw.get("dedup", {}) or {}
            bn = raw.get("breaking_news", {}) or {}
            crawl = raw.get("crawl", {}) or {}

            csv_state = "ON" if bool(self.auto_save_csv.get()) else "OFF"
            out_dir = str(self.output_dir)
            dedup_th = float(dd.get("similarity_threshold", 0.92))
            max_inflight = int(conc.get("max_in_flight_requests", 20))
            max_req = int(rl.get("max_requests_per_period", 3))
            per_s = float(rl.get("period_seconds", 1.0))
            break_min = float(bn.get("min_score", 0.55))
            crawl_cap = int(crawl.get("max_articles_per_run", 120))

            self._settings_summary.set(
                f"CSV(per-source): {csv_state} | Out: {out_dir} | Concurrency: {max_inflight} | Rate: {max_req}/{per_s:g}s | Dedup≥{dedup_th:.2f} | Breaking≥{break_min:.2f} | Crawl cap: {crawl_cap}"
            )
        except Exception:
            self._settings_summary.set("")

    def _build_live(self) -> None:
        top_outer = RoundedContainer(
            self.tab_live,
            radius=12,
            bg=self._app_bg,
            fill=self._card_bg,
            outline="#e2e8f0",
            padding=(12, 10),
            inner_style="ToolbarCard.TFrame",
        )
        top = top_outer.inner
        top_outer.pack(fill="x", padx=12, pady=(12, 10))

        ttk.Button(top, text="Refresh", command=self.refresh_live, style="Primary.TButton").pack(side="left")
        ttk.Button(top, text="Fetch now", command=self.fetch_now, style="Primary.TButton").pack(side="left", padx=(8, 0))

        ttk.Button(top, text="Open folder", command=self._open_output_folder, style="Icon.TButton").pack(side="left", padx=(12, 0))
        ttk.Checkbutton(
            top,
            text="Auto-save CSV",
            variable=self.auto_save_csv,
            command=self._on_autosave_toggle,
            style="Toggle.TCheckbutton",
        ).pack(side="left", padx=(8, 0))

        self.live_source_filter = tk.StringVar(value="All")
        ttk.Label(top, text="Source:", style="Muted.TLabel").pack(side="left", padx=(16, 6))
        self.live_source_combo = ttk.Combobox(
            top,
            textvariable=self.live_source_filter,
            values=["All"],
            width=22,
            state="readonly",
        )
        self.live_source_combo.bind("<<ComboboxSelected>>", lambda _e: self.refresh_live())
        self.live_source_combo.pack(side="left")
        self.live_running = tk.BooleanVar(value=True)
        ttk.Checkbutton(top, text="Live running", variable=self.live_running, command=self._toggle_live).pack(side="left", padx=(16, 0))

        self.poll_seconds = tk.IntVar(value=120)
        ttk.Label(top, text="Poll (s):", style="Muted.TLabel").pack(side="left", padx=(16, 6))
        ttk.Spinbox(top, from_=30, to=3600, textvariable=self.poll_seconds, width=6).pack(side="left")

        self.live_filter_dup = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            top,
            text="Hide duplicates",
            variable=self.live_filter_dup,
            command=self.refresh_live,
        ).pack(side="left", padx=(16, 0))

        self.live_limit = tk.IntVar(value=200)
        ttk.Label(top, text="Max shown:", style="Muted.TLabel").pack(side="left", padx=(16, 6))
        ttk.Spinbox(top, from_=20, to=2000, textvariable=self.live_limit, width=6, command=self.refresh_live).pack(side="left")

        ttk.Label(top, textvariable=self._settings_summary, style="Settings.TLabel").pack(side="right")

        ttk.Separator(self.tab_live).pack(fill="x", padx=12)
        self.live_list = ScrollFrame(self.tab_live, max_content_width=1020)
        self.live_list.canvas.configure(background=self._app_bg)
        self.live_list.pack(fill="both", expand=True, padx=12, pady=12)

    def _build_saved(self) -> None:
        top_outer = RoundedContainer(
            self.tab_saved,
            radius=12,
            bg=self._app_bg,
            fill=self._card_bg,
            outline="#e2e8f0",
            padding=(12, 10),
            inner_style="ToolbarCard.TFrame",
        )
        top = top_outer.inner
        top_outer.pack(fill="x", padx=12, pady=(12, 10))

        ttk.Button(top, text="Refresh", command=self.refresh_saved, style="Primary.TButton").pack(side="left")
        ttk.Button(top, text="Open folder", command=self._open_output_folder, style="Icon.TButton").pack(side="left", padx=(12, 0))
        ttk.Checkbutton(
            top,
            text="Auto-save CSV",
            variable=self.auto_save_csv,
            command=self._on_autosave_toggle,
            style="Toggle.TCheckbutton",
        ).pack(side="left", padx=(8, 0))

        ttk.Label(top, textvariable=self._settings_summary, style="Settings.TLabel").pack(side="right")

        ttk.Separator(self.tab_saved).pack(fill="x", padx=12)
        self.saved_list = ScrollFrame(self.tab_saved, max_content_width=1020)
        self.saved_list.canvas.configure(background=self._app_bg)
        self.saved_list.pack(fill="both", expand=True, padx=12, pady=12)

    def _build_breaking(self) -> None:
        top_outer = RoundedContainer(
            self.tab_breaking,
            radius=12,
            bg=self._app_bg,
            fill=self._card_bg,
            outline="#e2e8f0",
            padding=(12, 10),
            inner_style="ToolbarCard.TFrame",
        )
        top = top_outer.inner
        top_outer.pack(fill="x", padx=12, pady=(12, 10))

        ttk.Button(top, text="Refresh", command=self.refresh_breaking, style="Primary.TButton").pack(side="left")

        ttk.Button(top, text="Open folder", command=self._open_output_folder, style="Icon.TButton").pack(side="left", padx=(12, 0))
        ttk.Checkbutton(
            top,
            text="Auto-save CSV",
            variable=self.auto_save_csv,
            command=self._on_autosave_toggle,
            style="Toggle.TCheckbutton",
        ).pack(side="left", padx=(8, 0))

        self.breaking_source_filter = tk.StringVar(value="All")
        ttk.Label(top, text="Source:", style="Muted.TLabel").pack(side="left", padx=(16, 6))
        self.breaking_source_combo = ttk.Combobox(
            top,
            textvariable=self.breaking_source_filter,
            values=["All"],
            width=22,
            state="readonly",
        )
        self.breaking_source_combo.bind("<<ComboboxSelected>>", lambda _e: self.refresh_breaking())
        self.breaking_source_combo.pack(side="left")

        self.breaking_limit = tk.IntVar(value=120)
        ttk.Label(top, text="Max shown:", style="Muted.TLabel").pack(side="left", padx=(16, 6))
        ttk.Spinbox(
            top,
            from_=10,
            to=2000,
            textvariable=self.breaking_limit,
            width=6,
            command=self.refresh_breaking,
        ).pack(side="left")

        ttk.Label(top, textvariable=self._settings_summary, style="Settings.TLabel").pack(side="right")

        ttk.Separator(self.tab_breaking).pack(fill="x", padx=12)
        self.breaking_list = ScrollFrame(self.tab_breaking, max_content_width=1020)
        self.breaking_list.canvas.configure(background=self._app_bg)
        self.breaking_list.pack(fill="both", expand=True, padx=12, pady=12)

    def _clear(self, frame: ttk.Frame) -> None:
        for child in list(frame.winfo_children()):
            child.destroy()

    def _article_from_row(self, row: pd.Series) -> Article:
        published_at = _parse_dt(row.get("published_at"))
        return Article(
            source=_safe_str(row.get("source")),
            title=_safe_str(row.get("title")),
            url=_safe_str(row.get("url")),
            published_at=published_at,
            summary=_safe_str(row.get("summary")) or None,
            text=_safe_str(row.get("text")) or None,
            authors=list(row.get("authors") or []) if isinstance(row.get("authors"), list) else [],
            tags=list(row.get("tags") or []) if isinstance(row.get("tags"), list) else _split_listish(row.get("tags")),
            entities=list(row.get("entities") or []) if isinstance(row.get("entities"), list) else [],
            keywords=list(row.get("keywords") or []) if isinstance(row.get("keywords"), list) else _split_listish(row.get("keywords")),
            score=float(row.get("score") or 0.0),
            is_duplicate=bool(row.get("is_duplicate") or False),
            duplicate_of_url=_safe_str(row.get("duplicate_of_url")) or None,
        )

    def _add_article_card(
        self,
        parent: ttk.Frame,
        article: Article,
        *,
        on_save: Callable[[Article], None] | None = None,
        on_remove: Callable[[str], None] | None = None,
        saved_mode: bool,
    ) -> None:
        card_outer = RoundedContainer(
            parent,
            radius=12,
            bg=self._app_bg,
            fill=self._card_bg,
            outline="#e2e8f0",
            padding=(14, 12),
        )
        card_outer.pack(fill="x", pady=8)
        card = card_outer.inner

        header = ttk.Frame(card, style="Card.TFrame")
        header.pack(fill="x")

        title = ttk.Label(header, text=article.title or "(no title)", style="Title.TLabel")
        title.pack(side="left", anchor="w", fill="x", expand=True)

        s = float(article.score or 0.0)
        if s >= 0.70:
            badge_bg = "#16a34a"
        elif s >= 0.45:
            badge_bg = "#f59e0b"
        else:
            badge_bg = "#64748b"
        badge = tk.Label(
            header,
            text=f"{s:.2f}",
            fg="white",
            bg=badge_bg,
            padx=8,
            pady=2,
            font=("Segoe UI", 9, "bold"),
        )
        badge.pack(side="right", anchor="e")

        meta = f"{article.source}"
        if article.published_at:
            meta += f" | {article.published_at}"
        if article.tags:
            meta += f" | tags: {', '.join(article.tags[:8])}"
        if article.is_duplicate:
            meta += " | DUPLICATE"
        meta_lbl = ttk.Label(card, text=meta, style="Meta.TLabel")
        meta_lbl.pack(anchor="w", pady=(6, 0))

        btn_row = ttk.Frame(card, style="Card.TFrame")
        btn_row.pack(fill="x", pady=(10, 0))

        ttk.Button(
            btn_row,
            text="Open",
            command=lambda: self.open_article_window(article),
            style="Secondary.TButton",
        ).pack(side="left")
        ttk.Button(
            btn_row,
            text="Open link",
            command=lambda: _open_link(article.url),
            style="Secondary.TButton",
        ).pack(side="left", padx=(8, 0))

        if saved_mode:
            ttk.Button(
                btn_row,
                text="Remove",
                command=lambda: (on_remove(article.url) if on_remove else None),
                style="Danger.TButton",
            ).pack(side="right")
        else:
            ttk.Button(
                btn_row,
                text="Save",
                command=lambda: (on_save(article) if on_save else None),
                style="Primary.TButton",
            ).pack(side="right")

        # make card clickable too
        for w in (card, title):
            try:
                w.configure(cursor="hand2")
            except Exception:
                pass
            w.bind("<Button-1>", lambda e, a=article: self.open_article_window(a))

        def _sync_wrap(_e: tk.Event | None = None) -> None:
            try:
                # Wrap based on available card width.
                w = int(card.winfo_width() or 0)
                if w <= 0:
                    return
                title.configure(wraplength=max(280, w - 120))
                meta_lbl.configure(wraplength=max(320, w - 40))
                card_outer.after_idle(card_outer._sync_height)
            except Exception:
                return

        card.bind("<Configure>", _sync_wrap)
        self.root.after(0, _sync_wrap)
        card_outer.after_idle(card_outer._sync_height)

    def refresh_live(self) -> None:
        self.status.set("Loading Live...")
        self.root.update_idletasks()

        self._clear(self.live_list.inner)

        items = list(self.live_articles)
        # newest first
        # Update source dropdown options from current live set
        sources = sorted({a.source for a in items if a.source})
        values = ["All"] + sources
        if tuple(self.live_source_combo.cget("values")) != tuple(values):
            self.live_source_combo.configure(values=values)
            if self.live_source_filter.get() not in values:
                self.live_source_filter.set("All")

        selected_source = self.live_source_filter.get()
        if selected_source and selected_source != "All":
            items = [a for a in items if a.source == selected_source]

        def _sort_key(a: Article) -> tuple[float, float]:
            score = float(a.score or 0.0)
            dt = a.published_at
            ts = float("-inf")
            if dt is not None:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                try:
                    ts = dt.astimezone(timezone.utc).timestamp()
                except Exception:
                    ts = float("-inf")
            return (score, ts)

        items.sort(key=_sort_key, reverse=True)
        if self.live_filter_dup.get():
            items = [a for a in items if not a.is_duplicate]

        limit = int(self.live_limit.get() or 200)
        items = items[:limit]

        if not items:
            ttk.Label(self.live_list.inner, text="No live articles yet. Waiting for RSS...").pack(
                anchor="w", padx=12, pady=12
            )
            self.status.set("Live empty")
            return

        def on_save(article: Article) -> None:
            save_article(self.output_dir, article)
            self.status.set("Saved")
            self.refresh_saved()

        for a in items:
            self._add_article_card(self.live_list.inner, a, on_save=on_save, saved_mode=False)

        self.status.set(f"Live loaded: {len(items)}")

    def refresh_saved(self) -> None:
        self.status.set("Loading Saved...")
        self.root.update_idletasks()

        self._clear(self.saved_list.inner)

        df = load_saved(self.output_dir)
        if df.empty:
            ttk.Label(self.saved_list.inner, text="No saved articles yet.").pack(anchor="w", padx=12, pady=12)
            self.status.set("Saved empty")
            return

        if "published_at" in df.columns:
            df["_published"] = pd.to_datetime(df["published_at"], utc=True, errors="coerce")
            df = df.sort_values("_published", ascending=False)

        def on_remove(url: str) -> None:
            remove_saved(self.output_dir, url)
            self.refresh_saved()
            self.status.set("Removed")

        for _, row in df.iterrows():
            a = self._article_from_row(row)
            self._add_article_card(self.saved_list.inner, a, on_remove=on_remove, saved_mode=True)

        self.status.set(f"Saved loaded: {len(df)}")

    def refresh_breaking(self) -> None:
        self.status.set("Loading Breaking...")
        self.root.update_idletasks()
        self._clear(self.breaking_list.inner)

        items = list(self.breaking_articles)

        sources = sorted({a.source for a in items if a.source})
        values = ["All"] + sources
        if tuple(self.breaking_source_combo.cget("values")) != tuple(values):
            self.breaking_source_combo.configure(values=values)
            if self.breaking_source_filter.get() not in values:
                self.breaking_source_filter.set("All")

        selected_source = self.breaking_source_filter.get()
        if selected_source and selected_source != "All":
            items = [a for a in items if a.source == selected_source]
        items.sort(key=lambda a: float(a.score or 0.0), reverse=True)

        limit = int(self.breaking_limit.get() or 120)
        items = items[:limit]

        if not items:
            ttk.Label(self.breaking_list.inner, text="No breaking articles yet.").pack(anchor="w", padx=12, pady=12)
            self.status.set("Breaking empty")
            return

        def on_save(article: Article) -> None:
            save_article(self.output_dir, article)
            self.status.set("Saved")
            self.refresh_saved()

        for a in items:
            self._add_article_card(self.breaking_list.inner, a, on_save=on_save, saved_mode=False)

        self.status.set(f"Breaking loaded: {len(items)}")

    def fetch_now(self) -> None:
        # Trigger an immediate cycle (still no auto-save).
        self.status.set("Fetching now...")
        persist = bool(self.auto_save_csv.get())

        def run_bg() -> None:
            try:
                arts = asyncio.run(
                    run_pipeline(
                        self.config_path,
                        self.sources_path,
                        max_items=50,
                        persist=persist,
                        quiet=True,
                        skip_urls=set(self._seen_urls),
                        recent_texts=list(self._recent_texts),
                        recent_urls=list(self._recent_urls),
                    )
                )
                if arts:
                    self._q.put(arts)
                self.root.after(
                    0,
                    lambda: self.status.set(
                        "Fetch cycle complete" + (" (saved to per-source CSVs)" if persist else "")
                    ),
                )
            except Exception as e:
                self.root.after(0, lambda: self.status.set(f"Fetch failed: {e}"))

        threading.Thread(target=run_bg, daemon=True).start()

    def _start_live_loop(self) -> None:
        # Background loop: poll RSS periodically and update in-memory Live list.
        def loop() -> None:
            while not self._stop.is_set():
                if not self.live_running.get():
                    self._stop.wait(timeout=0.5)
                    continue

                persist = bool(self.auto_save_csv.get())

                try:
                    arts = asyncio.run(
                        run_pipeline(
                            self.config_path,
                            self.sources_path,
                            max_items=50,
                            source_group="sources",
                            persist=persist,
                            quiet=True,
                            skip_urls=self._snapshot_seen_urls(),
                            recent_texts=self._snapshot_recent_texts(),
                            recent_urls=self._snapshot_recent_urls(),
                        )
                    )
                    if arts:
                        self._q.put(arts)
                except Exception:
                    # best-effort live loop
                    pass

                # Sleep with some jitter to avoid perfectly periodic behavior
                base = max(10, int(self.poll_seconds.get() or 120))
                sleep_for = float(base) * random.uniform(0.8, 1.2)
                self._stop.wait(timeout=sleep_for)

        threading.Thread(target=loop, daemon=True).start()
        self._start_breaking_loop()
        self.root.after(250, self._drain_queue)

    def _start_breaking_loop(self) -> None:
        # More frequent polling for breaking_sources (shown in Breaking tab).
        def loop() -> None:
            while not self._stop.is_set():
                if not self.live_running.get():
                    self._stop.wait(timeout=0.5)
                    continue

                persist = bool(self.auto_save_csv.get())

                try:
                    arts = asyncio.run(
                        run_pipeline(
                            self.config_path,
                            self.sources_path,
                            max_items=40,
                            source_group="breaking_sources",
                            persist=persist,
                            quiet=True,
                            skip_urls=self._snapshot_seen_urls(),
                            recent_texts=self._snapshot_recent_texts(),
                            recent_urls=self._snapshot_recent_urls(),
                        )
                    )
                    if arts:
                        self._q.put(arts)
                        for a in arts:
                            if not a.url:
                                continue
                            if is_breaking(self.cfg.raw, a):
                                with self._lock:
                                    self._breaking_by_url[a.url] = a
                                    self._breaking_dirty = True
                except Exception:
                    pass

                # breaking poll: shorter interval than normal
                base = max(15, int(self.cfg.raw.get("breaking_news", {}).get("poll_seconds", 30)))
                sleep_for = float(base) * random.uniform(0.8, 1.2)
                self._stop.wait(timeout=sleep_for)

        threading.Thread(target=loop, daemon=True).start()

    def _drain_queue(self) -> None:
        changed = False
        try:
            while True:
                batch = self._q.get_nowait()
                for a in batch:
                    if not a.url:
                        continue
                    with self._lock:
                        self._seen_urls.add(a.url)
                        self._live_by_url[a.url] = a
                        if is_breaking(self.cfg.raw, a):
                            self._breaking_by_url[a.url] = a
                            self._breaking_dirty = True
                # Update rolling recent window for dedup
                for a in batch:
                    if a.text and a.url:
                        with self._lock:
                            self._recent_texts.append(a.text)
                            self._recent_urls.append(a.url)
                win = int(self.cfg.raw.get("dedup", {}).get("compare_window", 500))
                with self._lock:
                    if len(self._recent_texts) > win:
                        self._recent_texts = self._recent_texts[-win:]
                        self._recent_urls = self._recent_urls[-win:]
                changed = True
        except queue.Empty:
            pass

        with self._lock:
            breaking_dirty = self._breaking_dirty
            if breaking_dirty:
                self._breaking_dirty = False

        if changed:
            self.live_articles = list(self._live_by_url.values())
            self.refresh_live()

        if changed or breaking_dirty:
            with self._lock:
                self.breaking_articles = list(self._breaking_by_url.values())
            self.refresh_breaking()

        if not self._stop.is_set():
            self.root.after(500, self._drain_queue)

    def _snapshot_seen_urls(self) -> set[str]:
        with self._lock:
            return set(self._seen_urls)

    def _snapshot_recent_texts(self) -> list[str]:
        with self._lock:
            return list(self._recent_texts)

    def _snapshot_recent_urls(self) -> list[str]:
        with self._lock:
            return list(self._recent_urls)


    def _toggle_live(self) -> None:
        if self.live_running.get():
            self.status.set("Live running")
        else:
            self.status.set("Live paused")

    def _on_close(self) -> None:
        self._stop.set()
        self.root.destroy()

    def open_article_window(self, article: Article) -> None:
        win = tk.Toplevel(self.root)
        win.title(article.title[:80] if article.title else "Article")
        win.geometry("1000x700")

        header = ttk.Frame(win, padding=10)
        header.pack(fill="x")

        ttk.Label(header, text=article.title or "(no title)", wraplength=900).pack(anchor="w")
        ttk.Label(
            header,
            text=f"{article.source} | {article.published_at or ''} | score: {article.score:.2f}",
        ).pack(anchor="w", pady=(4, 0))

        if article.tags:
            ttk.Label(header, text=f"Tags: {', '.join(article.tags)}", wraplength=950).pack(anchor="w", pady=(4, 0))

        btn_row = ttk.Frame(header)
        btn_row.pack(fill="x", pady=(8, 0))
        ttk.Button(btn_row, text="Open link", command=lambda: _open_link(article.url)).pack(side="left")
        ttk.Button(btn_row, text="Close", command=win.destroy).pack(side="right")

        body = ttk.Frame(win, padding=10)
        body.pack(fill="both", expand=True)

        txt = tk.Text(body, wrap="word")
        txt.insert("1.0", article.text or article.summary or "(no article text found)")
        txt.configure(state="disabled")
        sb = ttk.Scrollbar(body, orient="vertical", command=txt.yview)
        txt.configure(yscrollcommand=sb.set)
        txt.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")


def _open_link(url: str) -> None:
    if not url:
        return
    try:
        webbrowser.open(url)
    except Exception:
        return


def _split_listish(x: Any) -> list[str]:
    # CSV roundtrips lists as strings; handle basic cases.
    if x is None:
        return []
    if isinstance(x, list):
        return [str(i) for i in x]
    s = str(x).strip()
    if not s:
        return []
    # common patterns: "['a', 'b']" or "a,b"
    if s.startswith("[") and s.endswith("]"):
        s2 = s.strip("[]")
        parts = [p.strip().strip("'\"") for p in s2.split(",")]
        return [p for p in parts if p]
    return [p.strip() for p in s.split(",") if p.strip()]


def _read_any(path: Path) -> pd.DataFrame:
    suf = path.suffix.lower()
    if suf == ".csv" or suf == ".txt":
        return pd.read_csv(path)
    if suf == ".parquet":
        return pd.read_parquet(path)
    # fallback
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.read_parquet(path)


def run_gui(*, config_path: str = "config/config.yaml", sources_path: str = "config/sources.yaml") -> None:
    root = tk.Tk()
    try:
        ttk.Style().theme_use("clam")
    except Exception:
        pass

    style = ttk.Style()
    app_bg = "#f6f8fb"
    card_bg = "#ffffff"

    try:
        root.configure(background=app_bg)
    except Exception:
        pass

    # Nicer defaults on Windows.
    try:
        default_font = tkfont.nametofont("TkDefaultFont")
        default_font.configure(family="Segoe UI", size=10)
        root.option_add("*Font", default_font)
    except Exception:
        pass

    style.configure("App.TFrame", background=app_bg)
    style.configure("ToolbarCard.TFrame", background=card_bg)
    style.configure("StatusCard.TFrame", background=card_bg)

    style.configure("Muted.TLabel", background=app_bg, foreground="#475569")
    style.configure("StatusCard.TLabel", background=card_bg, foreground="#0f172a")

    style.configure("Card.TFrame", background=card_bg, relief="flat", borderwidth=0)
    style.configure("Title.TLabel", background=card_bg, foreground="#0f172a", font=("Segoe UI", 11, "bold"))
    style.configure("Meta.TLabel", background=card_bg, foreground="#475569")

    style.configure("Settings.TLabel", background=card_bg, foreground="#475569", font=("Segoe UI", 9))
    style.configure("Toggle.TCheckbutton", background=card_bg, padding=(6, 4))
    style.configure("Icon.TButton", padding=(8, 6))

    # Button styling: ttk doesn't support true border-radius on Windows, but
    # we can make them feel modern (flat, padded, hover/pressed colors).
    style.configure(
        "Primary.TButton",
        padding=(12, 7),
        foreground="white",
        background="#2563eb",
        borderwidth=0,
        focusthickness=0,
        focuscolor="none",
    )
    style.map(
        "Primary.TButton",
        background=[("active", "#1d4ed8"), ("pressed", "#1e40af")],
        foreground=[("disabled", "#e2e8f0")],
    )

    style.configure(
        "Secondary.TButton",
        padding=(12, 7),
        foreground="#0f172a",
        background="#e2e8f0",
        borderwidth=0,
        focusthickness=0,
        focuscolor="none",
    )
    style.map(
        "Secondary.TButton",
        background=[("active", "#cbd5e1"), ("pressed", "#94a3b8")],
        foreground=[("disabled", "#94a3b8")],
    )

    style.configure(
        "Danger.TButton",
        padding=(12, 7),
        foreground="white",
        background="#dc2626",
        borderwidth=0,
        focusthickness=0,
        focuscolor="none",
    )
    style.map(
        "Danger.TButton",
        background=[("active", "#b91c1c"), ("pressed", "#991b1b")],
        foreground=[("disabled", "#fecaca")],
    )

    style.configure("App.TNotebook", background=app_bg, borderwidth=0)
    style.configure("TNotebook", background=app_bg)
    style.configure("TNotebook.Tab", padding=(12, 8))

    try:
        root.tk.call("tk", "scaling", 1.15)
    except Exception:
        pass

    try:
        root.minsize(900, 560)
    except Exception:
        pass
    app = NewsApp(root, config_path=config_path, sources_path=sources_path)
    root.mainloop()


def main() -> int:
    run_gui()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
