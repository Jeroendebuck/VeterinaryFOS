async function loadCSV(path){
  const res = await fetch(path);
  if(!res.ok) throw new Error(`Failed to load ${path}`);
  const txt = await res.text();
  const [header, ...lines] = txt.trim().split(/\r?\n/);
  const cols = header.split(',');
  return lines.map(line => {
    const vals = [];
    let cur = '', inQuotes = false;
    for (let i=0;i<line.length;i++){
      const ch = line[i];
      if (ch === '"') { inQuotes = !inQuotes; continue; }
      if (ch === ',' && !inQuotes){ vals.push(cur); cur=''; continue; }
      cur += ch;
    }
    vals.push(cur);
    const row = {};
    cols.forEach((c, idx) => row[c] = vals[idx]);
    return row;
  });
}

const state = {
  unit: '',
  matrix: [],
  latest: [],
  gaps: [],
  treemap: []
};

// dashboard/app.js (but you said these files are at repo root now)
async function loadAll(){
  const base = './exports/';   // ← was '../exports/'
  const [m, l, g, t] = await Promise.all([
    loadCSV(base + 'critical_mass_matrix.csv'),
    loadCSV(base + 'unit_concept_latest.csv'),
    loadCSV(base + 'gaps_opportunities.csv'),
    loadCSV(base + 'portfolio_treemap.csv'),
  ]);
  // ...
}


function toNum(x){ const v = Number(x); return isNaN(v) ? null : v; }

function drawMatrix(unit){
  const rows = state.matrix.filter(r => r.unit_id === unit);
  const x = rows.map(r => toNum(r.lq));
  const y = rows.map(r => toNum(r.headcount));
  const text = rows.map(r => r.concept_id);
  const trace = { x, y, text, mode: 'markers', type: 'scatter', marker: { size: 10 } };
  const layout = {
    xaxis: { title: 'LQ (specialization)', zeroline: true },
    yaxis: { title: 'Headcount (active authors)', zeroline: true },
    margin: { l:50, r:10, t:10, b:40 }, paper_bgcolor:'#111827', plot_bgcolor:'#111827', font:{ color:'#e5e7eb' }
  };
  Plotly.newPlot('matrix', [trace], layout, {displayModeBar:false});
}

function drawTreemap(unit){
  const rows = state.treemap.filter(r => r.unit_id === unit);
  // build hierarchy by level using label path approximation
  const labels = rows.map(r => r.node);
  const parents = rows.map(r => Number(r.level) === 1 ? '' : ''); // flat for MVP
  const values = rows.map(r => toNum(r.size) || 0);
  const data = [{
    type: 'treemap', labels, parents, values,
    branchvalues: 'total', hovertemplate: '%{label}<br>%{value} works<extra></extra>'
  }];
  const layout = { margin: {l:10,r:10,t:10,b:10}, paper_bgcolor:'#111827', plot_bgcolor:'#111827', font:{color:'#e5e7eb'} };
  Plotly.newPlot('treemap', data, layout, {displayModeBar:false});
}

function drawGaps(unit){
  const tbody = document.querySelector('#gaps-table tbody');
  tbody.innerHTML = '';
  const rows = state.gaps.filter(r => r.unit_id === unit);
  rows.sort((a,b) => (toNum(b.gap_score)||0) - (toNum(a.gap_score)||0));
  for (const r of rows.slice(0, 100)){
    const tr = document.createElement('tr');
    const td = (v) => { const x = document.createElement('td'); x.textContent = v ?? ''; return x; };
    tr.append(
      td(r.concept_label || r.concept_id),
      td((toNum(r.lq)?.toFixed(2)) ?? ''),
      td(''),
      td((toNum(r.global_growth)?.toFixed(2)) ?? ''),
      td((toNum(r.gap_score)?.toFixed(2)) ?? '')
    );
    tbody.appendChild(tr);
  }
}

async function init(){
  await loadAll();
  const input = document.getElementById('unit-input');
  const btn = document.getElementById('apply-btn');
  btn.addEventListener('click', () => {
    state.unit = input.value.trim();
    if (!state.unit) return;
    drawMatrix(state.unit);
    drawTreemap(state.unit);
    drawGaps(state.unit);
  });
}

init();
