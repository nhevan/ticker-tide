/**
 * app.js — Ticker Tide Web UI
 *
 * Handles ticker/date selection, snapshot loading, card rendering,
 * sparkline drawing, and LLM analysis requests.
 * No build step — vanilla ES2020.
 */

/* ── DOM references ──────────────────────────────────────────────────────── */
const tickerInput = document.getElementById("ticker-input");
const dateInput = document.getElementById("date-input");
const loadBtn = document.getElementById("load-btn");
const cardsContainer = document.getElementById("cards-container");
const errorBanner = document.getElementById("error-banner");

/* ── State ───────────────────────────────────────────────────────────────── */
let currentTicker = null;
let currentDate = null;

/* ── Init ────────────────────────────────────────────────────────────────── */
document.addEventListener("DOMContentLoaded", () => {
  tickerInput.addEventListener("change", onTickerChange);
  tickerInput.addEventListener("blur", onTickerChange);
  loadBtn.addEventListener("click", onLoad);

  // LLM buttons
  for (const tf of ["daily", "weekly", "monthly"]) {
    document.getElementById(`${tf}-llm-btn`).addEventListener("click", () => onLlmClick(tf));
  }
});

/* ── Ticker change → fetch date range ───────────────────────────────────── */
async function onTickerChange() {
  const ticker = tickerInput.value.trim().toUpperCase();
  if (!ticker) return;
  try {
    const resp = await fetch(`/api/dates?ticker=${encodeURIComponent(ticker)}`);
    if (!resp.ok) return;
    const { min, max } = await resp.json();
    if (min) {
      dateInput.min = min;
      dateInput.max = max;
      if (!dateInput.value || dateInput.value < min || dateInput.value > max) {
        dateInput.value = max;
      }
    }
  } catch (err) {
    console.warn("Failed to fetch date range:", err);
  }
}

/* ── Load snapshot ───────────────────────────────────────────────────────── */
async function onLoad() {
  const ticker = tickerInput.value.trim().toUpperCase();
  const date = dateInput.value;
  if (!ticker || !date) {
    showError("Please select a ticker and a date.");
    return;
  }

  currentTicker = ticker;
  currentDate = date;

  loadBtn.disabled = true;
  loadBtn.textContent = "Loading…";
  hideError();

  try {
    const resp = await fetch(
      `/api/snapshot?ticker=${encodeURIComponent(ticker)}&date=${encodeURIComponent(date)}`
    );
    if (resp.status === 404) {
      showError(`No data found for ticker "${ticker}".`);
      return;
    }
    if (!resp.ok) {
      showError("Failed to load snapshot. Please try again.");
      return;
    }
    const snapshot = await resp.json();
    renderSnapshot(snapshot);
    cardsContainer.classList.remove("hidden");
  } catch (err) {
    showError("Network error loading snapshot.");
    console.error(err);
  } finally {
    loadBtn.disabled = false;
    loadBtn.textContent = "Load";
  }
}

/* ── Render ──────────────────────────────────────────────────────────────── */
function renderSnapshot(snapshot) {
  renderDailyCard(snapshot.daily);
  renderTimeframeCard("weekly", snapshot.weekly);
  renderTimeframeCard("monthly", snapshot.monthly);
  // Reset LLM outputs
  for (const tf of ["daily", "weekly", "monthly"]) {
    const out = document.getElementById(`${tf}-llm-output`);
    out.classList.add("hidden");
    out.textContent = "";
    document.getElementById(`${tf}-llm-btn`).disabled = false;
  }
  // Reset daily-only enrichment sections
  document.getElementById("daily-flip-badge").classList.add("hidden");
  document.getElementById("daily-earnings").classList.add("hidden");
  document.getElementById("daily-why").classList.add("hidden");
}

function renderDailyCard(data) {
  const period = document.getElementById("daily-period");
  const body = document.getElementById("daily-body");
  const noData = body.querySelector(".no-data-msg");
  const cardData = body.querySelector(".card-data");

  period.textContent = data.resolved_period || "";

  if (!data.data_available) {
    noData.classList.remove("hidden");
    cardData.classList.add("hidden");
    return;
  }
  noData.classList.add("hidden");
  cardData.classList.remove("hidden");

  // Signal badge
  const signalEl = document.getElementById("daily-signal");
  const sig = data.signal || "N/A";
  signalEl.textContent = sig;
  signalEl.className = `signal-badge ${sig}`;

  // Confidence
  const conf = data.confidence != null ? `${data.confidence.toFixed(1)}%` : "N/A";
  document.getElementById("daily-confidence").textContent = `Confidence: ${conf}`;

  // Composite + calibrated
  const comp = data.composite_score != null ? data.composite_score.toFixed(1) : "N/A";
  const cal = data.calibrated_score != null ? data.calibrated_score.toFixed(2) : "N/A";
  document.getElementById("daily-composite").textContent = comp;
  document.getElementById("daily-calibrated").textContent = cal;

  // Category bars
  renderCategoryBars("daily-categories", data.categories, data.scores || {});

  // Sparkline
  drawSparkline("daily", data.sparkline || []);

  // Why bullets (above patterns)
  renderWhyBullets(data.key_signals || []);

  // Patterns
  renderPatterns("daily-patterns", data.patterns || []);

  // Earnings row (below score row, above category bars)
  renderEarningsSection(data.earnings || null);

  // Signal flip badge (in card header)
  renderSignalFlipBadge(data.signal_flip || null);
}

function renderTimeframeCard(timeframe, data) {
  const period = document.getElementById(`${timeframe}-period`);
  const body = document.getElementById(`${timeframe}-body`);
  const noData = body.querySelector(".no-data-msg");
  const cardData = body.querySelector(".card-data");

  let periodText = data.resolved_period_label || data.resolved_period || "";
  if (data.is_fallback) periodText += " (most recent closed)";
  period.textContent = periodText;

  if (!data.data_available) {
    noData.classList.remove("hidden");
    cardData.classList.add("hidden");
    return;
  }
  noData.classList.add("hidden");
  cardData.classList.remove("hidden");

  // Composite
  const comp = data.composite_score != null ? data.composite_score.toFixed(1) : "N/A";
  document.getElementById(`${timeframe}-composite`).textContent = comp;

  // Category bars
  const catContainer = document.getElementById(`${timeframe}-categories`);
  // For monthly, preserve the info tooltip element and clear the rest
  if (timeframe === "monthly") {
    const tooltip = catContainer.querySelector(".monthly-cdl-info");
    catContainer.innerHTML = "";
    if (tooltip) catContainer.appendChild(tooltip);
  }
  renderCategoryBars(`${timeframe}-categories`, data.categories, data.scores || {});

  // Sparkline
  drawSparkline(timeframe, data.sparkline || []);

  // Patterns
  renderPatterns(`${timeframe}-patterns`, data.patterns || []);
}

/* ── Category bars ───────────────────────────────────────────────────────── */
function renderCategoryBars(containerId, categories, scores) {
  const container = document.getElementById(containerId);
  // Remove existing bar rows (keep monthly tooltip if present)
  const tooltip = container.querySelector(".monthly-cdl-info");
  container.innerHTML = "";
  if (tooltip) container.appendChild(tooltip);

  for (const cat of categories) {
    const score = scores[cat];
    const row = document.createElement("div");
    row.className = "cat-bar-row";

    const label = document.createElement("span");
    label.className = "cat-bar-label";
    label.textContent = _catLabel(cat);

    const track = document.createElement("div");
    track.className = "cat-bar-track";
    const fill = document.createElement("div");

    if (score == null) {
      fill.className = "cat-bar-fill zero";
      fill.style.width = "0%";
    } else if (score >= 0) {
      fill.className = "cat-bar-fill pos";
      fill.style.width = `${Math.min(score / 100 * 50, 50)}%`;
    } else {
      fill.className = "cat-bar-fill neg";
      fill.style.width = `${Math.min(Math.abs(score) / 100 * 50, 50)}%`;
    }
    track.appendChild(fill);

    const value = document.createElement("span");
    value.className = "cat-bar-value";
    value.textContent = score != null ? score.toFixed(1) : "N/A";

    row.appendChild(label);
    row.appendChild(track);
    row.appendChild(value);
    container.appendChild(row);
  }
}

function _catLabel(cat) {
  const labels = {
    trend: "Trend", momentum: "Momentum", volume: "Volume",
    volatility: "Volatility", candlestick: "Candles",
    structural: "Structural", sentiment: "Sentiment",
    fundamental: "Fundamental", macro: "Macro",
  };
  return labels[cat] || cat;
}

/* ── Sparkline ───────────────────────────────────────────────────────────── */
const SPARK_CAPTIONS = {
  daily: "Close · last 15 days",
  weekly: "Close · last 6 weeks",
  monthly: "Close · last 6 months",
};

function formatPrice(value) {
  if (value == null || Number.isNaN(value)) return "";
  return `$${value.toFixed(2)}`;
}

function drawSparkline(timeframe, data) {
  const canvas = document.getElementById(`${timeframe}-sparkline`);
  const captionEl = document.getElementById(`${timeframe}-spark-caption`);
  const leftEl = document.getElementById(`${timeframe}-spark-left`);
  const rightEl = document.getElementById(`${timeframe}-spark-right`);
  if (!canvas) return;

  const points = (data || []).filter((d) => d != null && d.close != null);
  const closes = points.map((d) => d.close);

  if (captionEl) captionEl.textContent = SPARK_CAPTIONS[timeframe] || "";
  if (leftEl) leftEl.textContent = closes.length ? formatPrice(closes[0]) : "";
  if (rightEl) rightEl.textContent = closes.length ? formatPrice(closes[closes.length - 1]) : "";

  const ctx = canvas.getContext("2d");
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.offsetWidth || 300;
  const h = canvas.offsetHeight || 60;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, w, h);

  if (closes.length < 2) return;

  const minVal = Math.min(...closes);
  const maxVal = Math.max(...closes);
  const range = maxVal - minVal || 1;
  const trendUp = closes[closes.length - 1] >= closes[0];

  const pad = 4;
  const xStep = (w - pad * 2) / (closes.length - 1);

  ctx.beginPath();
  ctx.strokeStyle = trendUp ? "#3ba776" : "#d65a5a";
  ctx.lineWidth = 1.5;
  ctx.lineJoin = "round";

  for (let idx = 0; idx < closes.length; idx++) {
    const x = pad + idx * xStep;
    const y = h - pad - ((closes[idx] - minVal) / range) * (h - pad * 2);
    if (idx === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  }
  ctx.stroke();
}

/* ── Patterns ────────────────────────────────────────────────────────────── */
function renderPatterns(listId, patterns) {
  const ul = document.getElementById(listId);
  ul.innerHTML = "";
  if (!patterns || patterns.length === 0) {
    const li = document.createElement("li");
    li.className = "none-msg";
    li.textContent = "— none detected";
    ul.appendChild(li);
    return;
  }
  for (const pat of patterns) {
    const li = document.createElement("li");
    const dir = (pat.direction || "").toLowerCase();
    const cls = dir === "bullish" ? "pattern-bullish" : dir === "bearish" ? "pattern-bearish" : "";
    li.className = cls;
    const strength = pat.strength != null ? ` (str: ${pat.strength})` : "";
    li.textContent = `${pat.pattern_name || "Unknown"}${strength}`;
    ul.appendChild(li);
  }
}

/* ── Why bullets ─────────────────────────────────────────────────────────── */
/**
 * Render the "Why" bullet list above the Patterns section on the daily card.
 * Hides the entire .why-section block if keySignals is empty or missing.
 *
 * @param {string[]} keySignals - Array of signal description strings.
 */
function renderWhyBullets(keySignals) {
  const section = document.getElementById("daily-why");
  const ul = document.getElementById("daily-why-list");
  ul.innerHTML = "";

  if (!keySignals || keySignals.length === 0) {
    section.classList.add("hidden");
    return;
  }

  for (const signal of keySignals) {
    const li = document.createElement("li");
    li.textContent = signal;
    ul.appendChild(li);
  }
  section.classList.remove("hidden");
}

/* ── Earnings section ────────────────────────────────────────────────────── */
/**
 * Render the earnings row (next + last surprise) on the daily card.
 * Hides the .earnings-section block if both next and last_surprise are null.
 *
 * @param {{ next: object|null, last_surprise: object|null }|null} earnings
 */
function renderEarningsSection(earnings) {
  const section = document.getElementById("daily-earnings");
  const nextEl = document.getElementById("daily-earnings-next");
  const lastEl = document.getElementById("daily-earnings-last");

  nextEl.textContent = "";
  lastEl.textContent = "";
  lastEl.className = "earnings-line";

  if (!earnings || (!earnings.next && !earnings.last_surprise)) {
    section.classList.add("hidden");
    return;
  }

  if (earnings.next) {
    const { date, days_until, estimated_eps } = earnings.next;
    const dateLabel = _formatEarningsDate(date);
    const daysLabel = days_until != null ? ` (in ${days_until}d)` : "";
    const epsLabel = estimated_eps != null ? ` · est $${estimated_eps.toFixed(2)}` : "";
    nextEl.textContent = `Next earnings: ${dateLabel}${daysLabel}${epsLabel}`;
  }

  if (earnings.last_surprise) {
    const { date, actual_eps, surprise, beat } = earnings.last_surprise;
    const dateLabel = _formatEarningsDate(date);
    if (surprise != null) {
      const sign = surprise >= 0 ? "+" : "";
      const beatWord = beat ? "beat" : "miss";
      lastEl.textContent = `Last surprise: $${sign}${surprise.toFixed(2)} ${beatWord} (${dateLabel})`;
      lastEl.classList.add(beat ? "earnings-beat" : "earnings-miss");
    } else {
      const epsLabel = actual_eps != null ? `$${actual_eps.toFixed(2)}` : "N/A";
      lastEl.textContent = `Last earnings: ${epsLabel} (${dateLabel})`;
    }
  }

  section.classList.remove("hidden");
}

/**
 * Format an ISO date string (YYYY-MM-DD) to a short label like "Apr 30".
 *
 * @param {string} isoDate - ISO date string.
 * @returns {string} Formatted short date.
 */
function _formatEarningsDate(isoDate) {
  try {
    const [year, month, day] = isoDate.split("-").map(Number);
    const months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
    return `${months[month - 1]} ${day}`;
  } catch (_) {
    return isoDate;
  }
}

/* ── Signal flip badge ───────────────────────────────────────────────────── */

/**
 * Mapping of signal transitions to display properties (color CSS class and glyph).
 * Convention: color follows direction toward/away from BULLISH.
 * Key format: "PREV→NEW".
 */
const _FLIP_STYLES = {
  "NEUTRAL→BULLISH":  { cls: "flip-up",   glyph: "↑" },
  "BEARISH→BULLISH":  { cls: "flip-up",   glyph: "↑" },
  "NEUTRAL→BEARISH":  { cls: "flip-down", glyph: "↓" },
  "BULLISH→BEARISH":  { cls: "flip-down", glyph: "↓" },
  "BEARISH→NEUTRAL":  { cls: "flip-up",   glyph: "→" },
  "BULLISH→NEUTRAL":  { cls: "flip-down", glyph: "→" },
};

/**
 * Render the signal flip badge next to the daily card period label.
 * Hides the badge if signalFlip is null.
 *
 * @param {{ date: string, previous_signal: string, new_signal: string, days_ago: number }|null} signalFlip
 */
function renderSignalFlipBadge(signalFlip) {
  const badge = document.getElementById("daily-flip-badge");
  badge.textContent = "";
  badge.className = "signal-flip-badge hidden";

  if (!signalFlip) return;

  const { previous_signal, new_signal, days_ago } = signalFlip;
  const transitionKey = `${previous_signal}→${new_signal}`;
  const style = _FLIP_STYLES[transitionKey] || { cls: "flip-neutral", glyph: "~" };

  const daysLabel = days_ago === 0 ? "today" : `${days_ago}d ago`;
  badge.textContent = `${style.glyph} ${new_signal} from ${previous_signal} (${daysLabel})`;
  badge.className = `signal-flip-badge ${style.cls}`;
}

/* ── LLM ─────────────────────────────────────────────────────────────────── */
async function onLlmClick(timeframe) {
  if (!currentTicker || !currentDate) return;

  const btn = document.getElementById(`${timeframe}-llm-btn`);
  const out = document.getElementById(`${timeframe}-llm-output`);

  btn.disabled = true;
  btn.textContent = "Analyzing…";
  out.textContent = "";
  out.classList.remove("hidden");
  out.textContent = "Generating analysis…";

  try {
    const resp = await fetch("/api/llm", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ticker: currentTicker, date: currentDate, timeframe }),
    });

    if (resp.status === 429) {
      out.textContent = "Analysis already requested. Please wait 60 seconds before requesting again.";
      return;
    }
    if (resp.status === 503) {
      const data = await resp.json();
      out.textContent = data.detail || "AI analysis temporarily unavailable.";
      return;
    }
    if (!resp.ok) {
      out.textContent = "Failed to generate analysis.";
      return;
    }

    const data = await resp.json();
    out.textContent = data.text || "No analysis returned.";
  } catch (err) {
    out.textContent = "Network error generating analysis.";
    console.error(err);
  } finally {
    btn.disabled = false;
    btn.textContent = "Ask AI";
  }
}

/* ── Error banner ────────────────────────────────────────────────────────── */
function showError(msg) {
  errorBanner.textContent = msg;
  errorBanner.classList.remove("hidden");
  setTimeout(() => errorBanner.classList.add("hidden"), 5000);
}

function hideError() {
  errorBanner.classList.add("hidden");
}
