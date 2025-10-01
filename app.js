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

// REPLACE your existing loadAll() with this:
async function loadAll() {
  const base = './exports/';

  try {
    const [matrix, latest, gaps, treemap] = await Promise.all([
      loadCSV(base + 'critical_mass_matrix.csv'),
      loadCSV(base + 'unit_concept_latest.csv'),
      loadCSV(base + 'gaps_opportunities.csv'),
      loadCSV(base + 'portfolio_treemap.csv'),
    ]);

    state.matrix = matrix;
    state.latest = latest;
    state.gaps = gaps;
    state.treemap = treemap;

    // Build human-readable labels from the latest view
    labelByConcept = new Map(
      state.latest.map(r => [r.concept_id, r.concept_label || r.concept_id])
    );
  } catch (err) {
    console.error(err);
    const msg = document.createElement('div');
    msg.style.cssText = 'background:#7f1d1d;color:#fecaca;padding:10px;border-radius:8px;margin:10px;';
    msg.textContent = 'Failed to load one or more CSVs from ./exports/. Check your Pages deploy & workflow.';
    document.body.prepend(msg);
    throw err;
  }
}



function toNum(x){ const v = Number(x); return isNaN(v) ? null : v; }


function drawMatrix(unit){
  const rows = state.matrix.filter(r => r.unit_id === unit);
  if (!rows.length) {
    Plotly.newPlot('matrix', [], {
      paper_bgcolor:'#111827', plot_bgcolor:'#111827', font:{color:'#e5e7eb'},
      annotations: [{text:'No data for this unit', x:0.5, y:0.5, showarrow:false}]
    }, {displayModeBar:false});
    return;
  }

  const x = rows.map(r => Number(r.lq));
  const y = rows.map(r => Number(r.headcount));
  const text = rows.map(r => labelByConcept.get(r.concept_id) || r.concept_id);

  const trace = {
    x, y, text,
    mode: 'markers',
    type: 'scatter',
    marker: { size: 10 },
    hovertemplate: '%{text}<br>LQ=%{x:.2f}<br>Headcount=%{y}<extra></extra>'
  };

  const maxY = Math.max(4, ...y.filter(n => Number.isFinite(n)));
  const maxX = Math.max(1, ...x.filter(n => Number.isFinite(n)));

  const shapes = [
    { type:'line', x0:1, x1:1, y0:0, y1:maxY, line:{ width:1, dash:'dot' } },
    { type:'line', x0:0, x1:maxX, y0:4, y1:4, line:{ width:1, dash:'dot' } },
  ];

  const layout = {
    xaxis: { title: 'LQ (specialization)', zeroline: false },
    yaxis: { title: 'Headcount (active authors)', zeroline: false },
    margin: { l:50, r:10, t:10, b:40 },
    paper_bgcolor:'#111827', plot_bgcolor:'#111827', font:{ color:'#e5e7eb' },
    shapes
  };
  Plotly.newPlot('matrix', [trace], layout, {displayModeBar:false});
}


// REPLACE your existing drawTreemap() with this:
function drawTreemap(unit){
  const rows = state.treemap.filter(r => r.unit_id === unit);
  if (!rows.length) {
    Plotly.newPlot('treemap', [], {
      paper_bgcolor:'#111827', plot_bgcolor:'#111827', font:{color:'#e5e7eb'},
      annotations: [{text:'No data for this unit', x:0.5, y:0.5, showarrow:false}]
    }, {displayModeBar:false});
    return;
  }
  const labels  = rows.map(r => r.node);
  const parents = rows.map(_ => ''); // flat
  const values  = rows.map(r => Number(r.size) || 0);

  const data = [{
    type: 'treemap',
    labels, parents, values,
    branchvalues: 'total',
    hovertemplate: '%{label}<br>%{value} works<extra></extra>'
  }];
  const layout = {
    margin: {l:10,r:10,t:10,b:10},
    paper_bgcolor:'#111827', plot_bgcolor:'#111827', font:{color:'#e5e7eb'}
  };
  Plotly.newPlot('treemap', data, layout, {displayModeBar:false});
}


// REPLACE your existing drawGaps() with this:
function fmt(v, d=2){ const n = Number(v); return Number.isFinite(n) ? n.toFixed(d) : ''; }

function drawGaps(unit){
  const tbody = document.querySelector('#gaps-table tbody');
  tbody.innerHTML = '';

  // headcount lookup from matrix
  const heads = new Map(
    state.matrix.filter(r => r.unit_id === unit).map(r => [r.concept_id, Number(r.headcount)||0])
  );

  const rows = state.gaps.filter(r => r.unit_id === unit);
  rows.sort((a,b) => (Number(b.gap_score)||0) - (Number(a.gap_score)||0));

  for (const r of rows.slice(0, 50)){
    const tr = document.createElement('tr');
    const label = labelByConcept.get(r.concept_id) || r.concept_id;
    const link = r.concept_id && r.concept_id.startsWith('http')
      ? `<a href="${r.concept_id}" target="_blank" rel="noopener">${label}</a>`
      : label;

    tr.innerHTML = `
      <td>${link}</td>
      <td>${fmt(r.lq)}</td>
      <td>${heads.get(r.concept_id) ?? ''}</td>
      <td>${fmt(r.global_growth)}</td>
      <td>${fmt(r.gap_score)}</td>
    `;
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
