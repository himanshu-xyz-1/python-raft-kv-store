class ThemeManager:
    LIGHT = {
        "bg_root": "#f8f9fa",
        "bg_surface": "#ffffff",
        "bg_secondary": "#f1f3f5",
        "border": "#e2e8f0",
        "text_primary": "#0f172a",
        "text_secondary": "#64748b",
        "accent": "#2563eb",
        "leader": "#10b981",
        "follower": "#3b82f6",
        "candidate": "#f59e0b",
        "dead": "#ef4444",
        "console_bg": "#0f172a",
        "console_text": "#e2e8f0",
        "btn_clear_bg": "#fee2e2",
        "btn_clear_fg": "#b91c1c"
    }

    DARK = {
        "bg_root": "#0b0f19",
        "bg_surface": "#111827",
        "bg_secondary": "#1f2937",
        "border": "#374151",
        "text_primary": "#f9fafb",
        "text_secondary": "#9ca3af",
        "accent": "#3b82f6",
        "leader": "#10b981",
        "follower": "#60a5fa",
        "candidate": "#fbbf24",
        "dead": "#f87171",
        "console_bg": "#030712",
        "console_text": "#f3f4f6",
        "btn_clear_bg": "#450a0a",
        "btn_clear_fg": "#fca5a5"
    }

    def __init__(self):
        self.is_dark = False

    @property
    def current(self):
        return self.DARK if self.is_dark else self.LIGHT

    def toggle(self):
        self.is_dark = not self.is_dark
        return self.is_dark
