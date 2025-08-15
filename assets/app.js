// ====== 你可以改成自己的 raw 檔位址 ======
const FACTORS_PRIMARY  = 'https://raw.githubusercontent.com/edwardchin811227/HK20/main/data/factors.csv';
const FACTORS_FALLBACK = 'https://raw.githubusercontent.com/edwardchin811227/HK20/main/data/factors.csv'; // 同一份也可
const HK20_PRIMARY     = 'https://raw.githubusercontent.com/edwardchin811227/HK20/main/data/hk20.csv';
const HK20_FALLBACK    = 'https://raw.githubusercontent.com/edwardchin811227/HK20/main/data/hk20.csv';

// ====== 小工具 ======
const $ = (sel) => document.querySelector(sel);

async function loadCsv(url){
  const r = await fetch(url, { cache:'no-store' });
  if(!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
  const text = await r.text();
  return new Promise((resolve,reject)=>{
    Papa.parse(text, {
      header: true,            // 以第一列為欄名
      dynamicTyping: true,     // 自動轉數值/布林
      skipEmptyLines: true,
      complete: (res)=> resolve(res.data),
      error: reject
    });
  });
}

async function loadWithFallback(primary, fallback){
  try{
    const rows = await loadCsv(primary);
    return { rows, from:'primary', url: primary };
  }catch(e1){
    console.warn('primary 失敗，改試 fallback：', e1);
    const rows = await loadCsv(fallback);
    return { rows, from:'fallback', url: fallback };
  }
}

function summarize(rows, name){
  if(!rows || !rows.length) return `${name}: 0 rows`;
  const cols = Object.keys(rows[0]);
  const dates = rows.map(r=>r.Date).filter(Boolean);
  const minD = dates[0], maxD = dates[dates.length-1];
  return `${name}: ${rows.length} rows, ${cols.length} cols, range: ${minD} → ${maxD}, cols: ${cols.join(', ')}`;
}

async function main(){
  const state = $('#state'); const msg = $('#msg'); const out = $('#out');

  try{
    msg.textContent = '載入中…';
    const [factors, hk20] = await Promise.all([
      loadWithFallback(FACTORS_PRIMARY, FACTORS_FALLBACK),
      loadWithFallback(HK20_PRIMARY, HK20_FALLBACK),
    ]);

    state.textContent = `狀態：OK（factors: ${factors.from}, hk20: ${hk20.from}）`;
    state.className = 'badge';
    msg.textContent = '✅ 成功載入資料。';

    const s1 = summarize(factors.rows, 'factors.csv');
    const s2 = summarize(hk20.rows, 'hk20.csv');
    out.textContent =
      s1 + '\n' + s2 +
      '\n\n已載入資料，可切換「顯示因子」及「只顯示強勢」。';

    // === Chart base ===
    const el = document.getElementById('chart');
    el.style.height = '480px';
    const chart = echarts.init(el);
    const dates = factors.rows.map(r => r.Date);

    function normalizeSeries(rows, key){
      const vals = rows.map(r=>r[key]).filter(v=>typeof v === 'number' && !isNaN(v));
      const min = Math.min(...vals);
      const max = Math.max(...vals);
      const range = max - min || 1;
      return rows.map(r => {
        const v = r[key];
        return typeof v === 'number' && !isNaN(v) ? (v - min) / range : null;
      });
    }

    // Factors
    const factorKeys = ['HSI','HSTECH','USDCNH','VHSI','BTC','Fused_macro'];
    const factorSeries = factorKeys.map(key => ({
      name: key,
      type: 'line',
      data: normalizeSeries(factors.rows, key),
      showSymbol: false,
    }));

    // Stocks
    const stockKeys = Object.keys(hk20.rows[0]).filter(k => k !== 'Date');
    const stockSeries = stockKeys.map(key => ({
      name: key,
      type: 'line',
      data: normalizeSeries(hk20.rows, key),
      showSymbol: false,
    }));

    // === Strength & momentum ===
    const fused = factorSeries.find(s=>s.name==='Fused_macro').data;
    const lastIdx = dates.length - 1;

    function calcSlope(arr){
      const n = arr.length;
      const xs = Array.from({length:n}, (_,i)=>i);
      const sumX = xs.reduce((a,b)=>a+b,0);
      const sumY = arr.reduce((a,b)=>a+b,0);
      const sumXY = arr.reduce((a,b,i)=>a + xs[i]*b,0);
      const sumXX = xs.reduce((a,b)=>a+b*b,0);
      return (n*sumXY - sumX*sumY) / (n*sumXX - sumX*sumX);
    }

    const slopes = stockSeries.map(s => ({ name: s.name, slope: calcSlope(s.data.slice(-20)) }));
    const sorted = [...slopes].sort((a,b)=>a.slope - b.slope);
    const quantile = new Map(sorted.map((s,i)=>[s.name, i/(sorted.length-1 || 1)]));

    const strong = new Set();
    slopes.forEach(({name})=>{
      const rel = stockSeries.find(s=>s.name===name).data[lastIdx] - fused[lastIdx];
      const mom = quantile.get(name);
      if(rel > 0 && mom > 0.5) strong.add(name);
    });
    out.textContent += '\n強勢股票：' + Array.from(strong).join(', ');

    // === Render ===
    function render(){
      const showFactors = $('#toggle-factors').checked;
      const onlyStrong = $('#toggle-strong').checked;
      let s = stockSeries;
      if(onlyStrong) s = s.filter(sr => strong.has(sr.name));
      const series = showFactors ? s.concat(factorSeries) : s;
      chart.setOption({
        title:{ text:'HK20 Strength & Momentum' },
        tooltip:{ trigger:'axis' },
        legend:{ top:0 },
        xAxis:{ type:'category', data: dates },
        yAxis:{ type:'value' },
        series,
      });
    }

    $('#toggle-factors').addEventListener('change', render);
    $('#toggle-strong').addEventListener('change', render);
    render();

  }catch(err){
    console.error(err);
    state.textContent = '狀態：讀取失敗'; state.className = 'badge';
    msg.textContent = '❌ 讀取或解析失敗。請打開瀏覽器主控台（F12）查看錯誤訊息。';
  }
}
window.addEventListener('DOMContentLoaded', main);

