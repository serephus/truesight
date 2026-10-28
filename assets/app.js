const state = {
  period: "day",
  heroes: [],
  search: "",
};

const $ = (id) => document.getElementById(id);

const el = {
  meta: $("meta"),
  error: $("error"),
  summary: $("summary"),
  heroRows: $("hero-rows"),
  search: $("search"),
  modal: $("modal"),
  modalBody: $("modal-body"),
};

const CDN = "https://cdn.cloudflare.steamstatic.com/apps/dota2/images/dota_react/heroes";

function showDetail(detail) {
  el.modalBody.innerHTML = detailHtml(detail);
  el.modal.hidden = false;
  document.body.style.overflow = "hidden";
}

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[c]);
}

function num(value, digits = 0) {
  return Number(value ?? 0).toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function heroIcon(icon, cls = "", iconUrl = "") {
  const url = iconUrl || (icon ? `${CDN}/${encodeURIComponent(icon)}.png` : "");
  if (!url) return `<span class="hero-icon ${cls}"></span>`;
  return `<img class="hero-icon ${cls}" loading="lazy" alt="" src="${url}">`;
}

function deltaHtml(delta) {
  const value = Number(delta || 0);
  if (Math.abs(value) < 0.05) return `<span class="delta-flat">0</span>`;
  const cls = value > 0 ? "delta-up" : "delta-down";
  return `<span class="${cls}">${value > 0 ? "+" : ""}${value.toFixed(0)}</span>`;
}

function formatTime(unix) {
  if (!unix) return "—";
  return new Date(unix * 1000).toLocaleString(undefined, {
    year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
  });
}

function showError(error) {
  el.error.textContent = error?.message || String(error);
  el.error.hidden = false;
  clearTimeout(showError.timer);
  showError.timer = setTimeout(() => { el.error.hidden = true; }, 8000);
}

async function api(path) {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;
    try {
      const body = await response.json();
      if (body?.error) message = body.error;
    } catch { /* ignore */ }
    throw new Error(message);
  }
  return response.json();
}

async function loadHeroes() {
  const data = await api(`/api/heroes?period=${state.period}`);
  state.heroes = data.heroes || [];
  if (state.heroes.length && state.heroes.every((hero) => !hero.name)) {
    showError(new Error("Hero names/icons are not loaded yet; check Steam GetHeroes access."));
  }
  renderMeta(data);
  renderLeaderboard();
}

function renderMeta(data) {
  const patch = data.patch ? `<span>Patch <b>${esc(data.patch)}</b></span>` : "";
  const adv = Number(data.radiant_advantage || 0);
  const windowEnd = data.window_end || data.computed_at;
  const windowRange = data.window_start && windowEnd
    ? `<b>${formatTime(data.window_start)}</b> → <b>${formatTime(windowEnd)}</b>`
    : "";
  const periodLabel = state.period[0].toUpperCase() + state.period.slice(1);
  el.meta.innerHTML = [
    `<span>Computed <b>${formatTime(data.computed_at)}</b></span>`,
    `<span>Sample <b>${num(data.matches)}</b> matches</span>`,
    `<span>Radiant edge <b>${adv >= 0 ? "+" : ""}${adv.toFixed(0)}</b></span>`,
    patch,
  ].filter(Boolean).join("");
  el.summary.innerHTML = [
    `<span><b>${periodLabel}</b> window${windowRange ? ` · ${windowRange}` : ""}</span>`,
    `<span>Ratings from Bradley–Terry over cross-team hero pairs</span>`,
    data.converged === false ? `<span class="delta-down">last fit did not fully converge</span>` : "",
  ].filter(Boolean).join("");
}

function filteredHeroes() {
  const query = state.search.trim().toLowerCase();
  return state.heroes
    .filter((h) => !query
      || h.localized_name.toLowerCase().includes(query)
      || h.name.toLowerCase().includes(query))
    .slice()
    .sort((a, b) => b.rating - a.rating);
}

function renderLeaderboard() {
  const heroes = filteredHeroes();
  if (!heroes.length) {
    el.heroRows.innerHTML = `<tr><td colspan="7" class="muted" style="padding:28px 16px">
      No ratings for this period yet. The collector needs some public ranked matches first.
    </td></tr>`;
    return;
  }
  el.heroRows.innerHTML = heroes.map((hero, index) => {
    const rank = hero.rank || index + 1;
    const rankClass = rank <= 3 ? `rank-${rank}` : "";
    const winRate = Math.max(0, Math.min(1, Number(hero.win_rate || 0)));
    return `<tr data-hero="${hero.hero_id}">
      <td class="num ${rankClass}">${rank}</td>
      <td>
        <div class="hero-cell">
          ${heroIcon(hero.icon, "", hero.icon_url)}
          <div>
            <div class="hero-name">${esc(hero.localized_name || `#${hero.hero_id}`)}</div>
            <div class="hero-sub">${esc(hero.primary_attr || "")}${hero.roles?.length ? ` · ${esc(hero.roles.slice(0, 2).join(", "))}` : ""}</div>
          </div>
        </div>
      </td>
      <td class="num"><span class="rating">${num(hero.rating)}</span></td>
      <td class="num rating-se">±${num(hero.rating_se)}</td>
      <td class="num">${deltaHtml(hero.delta)}</td>
      <td class="num">${num(hero.games)}</td>
      <td class="num">${(winRate * 100).toFixed(1)}%
        <span class="winrate-bar"><span style="width:${winRate * 100}%"></span></span>
      </td>
    </tr>`;
  }).join("");
  el.heroRows.querySelectorAll("tr[data-hero]").forEach((row) => {
    row.addEventListener("click", () => openHero(Number(row.dataset.hero)));
  });
}

async function openHero(heroId) {
  try {
    const detail = await api(`/api/heroes/${heroId}?period=${state.period}`);
    showDetail(detail);
  } catch (error) {
    showError(error);
  }
}

function detailHtml(detail) {
  const hero = detail.hero;
  const winRate = Number(hero.win_rate || 0) * 100;
  return `
    <div class="detail-head">
      ${heroIcon(hero.icon, "", hero.icon_url)}
      <div>
        <h2>${esc(hero.localized_name || `#${hero.hero_id}`)}</h2>
        <div class="sub">${esc(hero.attack_type || "")} ${hero.primary_attr ? `· ${esc(hero.primary_attr)}` : ""} ${hero.roles?.length ? `· ${esc(hero.roles.join(", "))}` : ""}</div>
      </div>
      <div class="big-rating">
        <div class="rating">${num(hero.rating)}</div>
        <div class="rating-se">±${num(hero.rating_se)} · ${deltaHtml(hero.delta)}</div>
      </div>
    </div>
    <div class="detail-stats">
      <span><b>${num(hero.games)}</b> games</span>
      <span><b>${winRate.toFixed(1)}%</b> win rate</span>
      <span><b>${num(hero.wins)}</b> wins</span>
      <span>${detail.window_start && detail.window_end
        ? `<b>${formatTime(detail.window_start)} → ${formatTime(detail.window_end)}</b> window`
        : `<b>${esc(state.period)}</b> window`}</span>
    </div>
    <h3 class="section">Rating history</h3>
    ${ratingChart(detail.history)}
    ${counterColumns(detail.beats, detail.loses_to)}
  `;
}

function ratingChart(history) {
  if (!history || history.length < 2) {
    return `<p class="muted">Not enough rating history yet.</p>`;
  }
  const width = 680;
  const height = 160;
  const pad = 10;
  const values = history.map((point) => Number(point.rating));
  let min = Math.min(...values);
  let max = Math.max(...values);
  if (max - min < 1) { max += 1; min -= 1; }
  const x = (index) => pad + (index / (history.length - 1)) * (width - pad * 2);
  const y = (value) => pad + (1 - (value - min) / (max - min)) * (height - pad * 2);
  const points = history.map((point, index) => `${x(index).toFixed(1)},${y(point.rating).toFixed(1)}`).join(" ");
  const area = `${pad},${height - pad} ${points} ${width - pad},${height - pad}`;
  return `<svg class="chart" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="Rating history">
    <defs>
      <linearGradient id="fill" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="#5eead4" stop-opacity=".28"/>
        <stop offset="100%" stop-color="#5eead4" stop-opacity="0"/>
      </linearGradient>
    </defs>
    <polygon points="${area}" fill="url(#fill)"/>
    <polyline points="${points}" fill="none" stroke="#5eead4" stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round"/>
  </svg>
  <div class="chart-note">Last ${history.length} snapshots · ${num(min)}–${num(max)} rating</div>`;
}

function counterColumns(beats, losesTo) {
  const column = (title, counters) => `
    <div class="counter-col">
      <h3>${title}</h3>
      ${counters.length ? counters.map((counter) => `
        <div class="counter-row">
          ${heroIcon(counter.icon, "small", counter.icon_url)}
          <span class="name">${esc(counter.localized_name)}</span>
          <span class="games">${num(counter.games)}g</span>
          <span class="rate">${(Number(counter.win_rate) * 100).toFixed(1)}%</span>
        </div>`).join("") : `<p class="muted">Not enough games yet.</p>`}
    </div>`;
  return `<div class="counter-grid">
    ${column("Beats", beats)}
    ${column("Loses to", losesTo)}
  </div>`;
}

function closeModal() {
  el.modal.hidden = true;
  document.body.style.overflow = "";
}

function setPeriod(period) {
  state.period = period;
  document.querySelectorAll("#periods .tab").forEach((tab) => {
    tab.classList.toggle("active", tab.dataset.period === period);
  });
  closeModal();
  loadHeroes().catch(showError);
}

document.querySelectorAll("#periods .tab").forEach((tab) => {
  tab.addEventListener("click", () => setPeriod(tab.dataset.period));
});
el.search.addEventListener("input", () => {
  state.search = el.search.value;
  renderLeaderboard();
});
document.querySelectorAll("[data-close]").forEach((node) => {
  node.addEventListener("click", closeModal);
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") closeModal();
});

loadHeroes().catch(showError);
setInterval(() => loadHeroes().catch(showError), 60_000);
