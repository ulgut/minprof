//! Self-contained HTML report generator.
//!
//! All CSS and JavaScript are inlined; the output is a single `.html` file
//! with no external dependencies.

use super::AnalysisOutput;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn fmt_bytes(b: u64) -> String {
    const MIB: u64 = 1 << 20;
    const KIB: u64 = 1 << 10;
    if b >= MIB      { format!("{:.2} MiB", b as f64 / MIB as f64) }
    else if b >= KIB { format!("{:.2} KiB", b as f64 / KIB as f64) }
    else             { format!("{b} B") }
}

fn pct(part: u64, total: u64) -> String {
    if total == 0 { return "0.0%".to_string(); }
    format!("{:.1}%", part as f64 / total as f64 * 100.0)
}

fn he(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

fn js_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"'  => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c    => out.push(c),
        }
    }
    out.push('"');
    out
}

// ── Static CSS ────────────────────────────────────────────────────────────────
// (Not inside format!() — curly braces in CSS are fine here.)

const CSS: &str = "
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,-apple-system,sans-serif;background:#f1f5f9;color:#1e293b;font-size:14px}
header{background:#1e293b;color:#f8fafc;padding:1.25rem 2rem}
header h1{font-size:1.25rem;font-weight:700}
header .sub{font-size:.8rem;color:#94a3b8;margin-top:.2rem}
nav{background:#fff;border-bottom:1px solid #e2e8f0;padding:.5rem 2rem;display:flex;gap:1.5rem;position:sticky;top:0;z-index:20;overflow-x:auto}
nav a{font-size:.8rem;color:#64748b;padding:.25rem 0;white-space:nowrap;border-bottom:2px solid transparent;transition:color .15s,border-color .15s}
nav a:hover{color:#6366f1;border-bottom-color:#6366f1}
main{max-width:1400px;margin:0 auto;padding:2rem;display:flex;flex-direction:column;gap:2rem}
section h2{font-size:.9rem;font-weight:700;color:#1e293b;margin-bottom:1rem;padding-bottom:.5rem;border-bottom:2px solid #6366f1;text-transform:uppercase;letter-spacing:.05em}
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:1rem}
.card{background:#fff;border-radius:10px;padding:1.25rem;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.card-label{font-size:.7rem;text-transform:uppercase;letter-spacing:.06em;color:#94a3b8}
.card-value{font-size:1.4rem;font-weight:700;color:#1e293b;margin-top:.25rem;line-height:1.2}
.card-sub{font-size:.75rem;color:#94a3b8;margin-top:.3rem}
.card.warn .card-value{color:#d97706}
.gc-grid{display:grid;grid-template-columns:1fr 1fr;gap:1rem}
.gc-box{background:#fff;border-radius:10px;padding:1.25rem;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.gc-box h3{font-size:.75rem;text-transform:uppercase;letter-spacing:.06em;color:#94a3b8;margin-bottom:.75rem}
.gc-row{display:flex;justify-content:space-between;align-items:center;padding:.4rem 0;border-bottom:1px solid #f1f5f9;font-size:.85rem}
.gc-row:last-child{border-bottom:none}
.gc-key{color:#475569}
.gc-val{font-weight:600;color:#1e293b}
.gc-val.warn{color:#d97706}
.gc-val.danger{color:#dc2626}
.suspects{display:flex;flex-direction:column;gap:.75rem}
.suspect{background:#fff;border-left:4px solid #f59e0b;border-radius:0 10px 10px 0;padding:1rem 1.25rem;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.suspect-n{font-size:.7rem;text-transform:uppercase;letter-spacing:.06em;color:#b45309;margin-bottom:.25rem}
.suspect-class{font-size:.95rem;font-weight:700;color:#1e293b;font-family:monospace;word-break:break-all}
.suspect-stats{display:flex;flex-wrap:wrap;gap:.75rem;margin-top:.5rem;font-size:.8rem}
.suspect-stats span{background:#fef3c7;color:#92400e;padding:.15rem .5rem;border-radius:4px}
.suspect-pattern{margin-top:.4rem;font-size:.8rem;color:#64748b;font-style:italic}
.treemap-wrap{background:#fff;border-radius:10px;padding:1.25rem;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.treemap-hint{font-size:.75rem;color:#94a3b8;margin-bottom:.75rem}
#tm-canvas{width:100%;border-radius:6px;cursor:pointer;display:block}
#tm-tooltip{position:fixed;background:rgba(15,23,42,.92);color:#f8fafc;padding:.5rem .75rem;border-radius:6px;font-size:.75rem;line-height:1.5;pointer-events:none;display:none;z-index:100;max-width:260px}
.chart-wrap{background:#fff;border-radius:10px;padding:1.25rem;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.chart-title{font-size:.75rem;text-transform:uppercase;letter-spacing:.06em;color:#94a3b8;margin-bottom:.75rem}
.tbl-wrap{background:#fff;border-radius:10px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.08)}
table{width:100%;border-collapse:collapse;font-size:.82rem}
th{background:#f8fafc;text-align:left;padding:.6rem 1rem;font-size:.7rem;text-transform:uppercase;letter-spacing:.06em;color:#94a3b8;border-bottom:1px solid #e2e8f0}
th.r{text-align:right}
td{padding:.55rem 1rem;border-bottom:1px solid #f1f5f9;color:#374151}
td.r{text-align:right;font-variant-numeric:tabular-nums}
td.mono{font-family:monospace;font-size:.8rem;word-break:break-all}
tr:last-child td{border-bottom:none}
tr:hover td{background:#fafafa}
@media(max-width:700px){.gc-grid{grid-template-columns:1fr}.cards{grid-template-columns:repeat(2,1fr)}}
";

// ── Static JavaScript (treemap engine + rendering) ────────────────────────────
// This is a const &str — not inside format!(), so JS {} syntax is fine as-is.

const JS_STATIC: &str = "
function fmtB(b){
  if(b>=1048576)return(b/1048576).toFixed(2)+' MiB';
  if(b>=1024)return(b/1024).toFixed(2)+' KiB';
  return b+' B';
}
function fmtPct(v,t){return t?((v/t)*100).toFixed(1)+'%':'0%';}
function he(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}

// Squarified treemap (Bruls et al.)
function worstRatio(row,rowArea,sideLen){
  const max=Math.max(...row.map(n=>n.sv));
  const min=Math.min(...row.map(n=>n.sv));
  if(!rowArea||!sideLen)return Infinity;
  const s2=sideLen*sideLen;
  return Math.max(s2*max/(rowArea*rowArea),rowArea*rowArea/(s2*min));
}
function squarify(nodes,x,y,w,h){
  if(!nodes.length)return[];
  const total=nodes.reduce((s,n)=>s+n.value,0);
  if(!total)return[];
  const areaScale=(w*h)/total;
  const scaled=nodes.map(n=>Object.assign({},n,{sv:n.value*areaScale}));
  const out=[];
  let rem=[...scaled],cx=x,cy=y,cw=w,ch=h;
  while(rem.length){
    const horiz=cw>=ch;
    const len=horiz?ch:cw;
    let row=[],rowA=0,worst=Infinity;
    for(const item of rem){
      const nr=[...row,item],na=rowA+item.sv;
      const w2=worstRatio(nr,na,len);
      if(row.length&&w2>worst)break;
      row=nr;rowA=na;worst=w2;
    }
    const dim=rowA/len;
    let off=0;
    for(const item of row){
      const frac=item.sv/rowA;
      if(horiz){out.push(Object.assign({},item,{x:cx,y:cy+off,w:dim,h:frac*len}));off+=frac*len;}
      else{out.push(Object.assign({},item,{x:cx+off,y:cy,w:frac*len,h:dim}));off+=frac*len;}
    }
    rem=rem.slice(row.length);
    if(horiz){cx+=dim;cw-=dim;}else{cy+=dim;ch-=dim;}
  }
  return out;
}

const PAL=['#6366f1','#0891b2','#059669','#d97706','#dc2626','#7c3aed','#0284c7','#16a34a','#ca8a04','#b91c1c','#0e7490','#4338ca','#047857','#b45309','#9333ea'];
function lighten(hex,amt){
  const n=parseInt(hex.slice(1),16);
  const r=Math.min(255,(n>>16)+amt);
  const g=Math.min(255,((n>>8)&0xff)+amt);
  const b=Math.min(255,(n&0xff)+amt);
  return'#'+[r,g,b].map(x=>x.toString(16).padStart(2,'0')).join('');
}

const canvas=document.getElementById('tm-canvas');
const tip=document.getElementById('tm-tooltip');
let DPR=window.devicePixelRatio||1;
let nodes=[];
let zoomPkg=null;

function resize(){
  const W=canvas.parentElement.clientWidth-40;
  const H=Math.max(300,Math.min(580,W*0.55));
  canvas.width=W*DPR;canvas.height=H*DPR;
  canvas.style.width=W+'px';canvas.style.height=H+'px';
  draw();
}

function draw(){
  const ctx=canvas.getContext('2d');
  const W=canvas.clientWidth,H=canvas.clientHeight;
  ctx.setTransform(DPR,0,0,DPR,0,0);
  ctx.clearRect(0,0,W,H);
  let items;
  if(zoomPkg===null){
    items=TREEMAP.map((p,i)=>Object.assign({},p,{color:PAL[i%PAL.length]}));
  }else{
    const pkg=TREEMAP[zoomPkg];
    const base=PAL[zoomPkg%PAL.length];
    items=(pkg.classes||[]).map((c,i)=>Object.assign({},c,{color:lighten(base,i*18)}));
  }
  nodes=squarify(items,0,0,W,H);
  for(const n of nodes){
    ctx.fillStyle=n.color;
    ctx.fillRect(n.x+1,n.y+1,n.w-2,n.h-2);
    if(n.w>50&&n.h>22){
      ctx.save();
      ctx.beginPath();
      ctx.rect(n.x+2,n.y+2,n.w-4,n.h-4);
      ctx.clip();
      const label=n.name.includes('.')?n.name.split('.').pop():n.name;
      const fs=Math.min(12,Math.max(9,n.w/12));
      ctx.fillStyle='rgba(255,255,255,0.95)';
      ctx.font='bold '+fs+'px system-ui';
      ctx.fillText(label,n.x+5,n.y+fs+3,n.w-10);
      if(n.h>38){
        ctx.fillStyle='rgba(255,255,255,0.7)';
        ctx.font=(fs-1)+'px system-ui';
        ctx.fillText(fmtB(n.value),n.x+5,n.y+fs*2+5,n.w-10);
      }
      ctx.restore();
    }
  }
  if(zoomPkg!==null){
    ctx.fillStyle='rgba(0,0,0,0.4)';
    ctx.fillRect(0,0,W,20);
    ctx.fillStyle='#fff';
    ctx.font='11px system-ui';
    ctx.fillText('< back  |  '+TREEMAP[zoomPkg].name,6,14);
  }
}

function hitNode(mx,my){
  return nodes.find(n=>mx>=n.x&&mx<n.x+n.w&&my>=n.y&&my<n.y+n.h)||null;
}

canvas.addEventListener('mousemove',function(e){
  const r=canvas.getBoundingClientRect();
  const mx=e.clientX-r.left,my=e.clientY-r.top;
  const n=hitNode(mx,my);
  if(n){
    tip.style.display='block';
    tip.style.left=(e.clientX+12)+'px';
    tip.style.top=(e.clientY+12)+'px';
    const inst=zoomPkg!==null?('<br>Instances: '+(n.instances||0).toLocaleString()):'';
    tip.innerHTML='<strong>'+he(n.name)+'</strong><br>Retained: '+fmtB(n.value)+' ('+fmtPct(n.value,TOTAL_BYTES)+' of heap)'+inst;
  }else{tip.style.display='none';}
});
canvas.addEventListener('mouseleave',function(){tip.style.display='none';});
canvas.addEventListener('click',function(e){
  const r=canvas.getBoundingClientRect();
  const mx=e.clientX-r.left,my=e.clientY-r.top;
  if(zoomPkg!==null){zoomPkg=null;draw();return;}
  const n=hitNode(mx,my);
  if(n){
    const idx=TREEMAP.findIndex(function(p){return p.name===n.name;});
    if(idx>=0&&TREEMAP[idx].classes&&TREEMAP[idx].classes.length>1){zoomPkg=idx;draw();}
  }
});
window.addEventListener('resize',resize);
resize();
";

// ── SVG horizontal bar chart ──────────────────────────────────────────────────

fn bar_chart(entries: &[(&str, u64)], total: u64) -> String {
    if entries.is_empty() { return String::new(); }
    let max_val = entries.iter().map(|e| e.1).max().unwrap_or(1).max(1);
    let row_h = 28usize;
    let label_w = 280usize;
    let bar_area = 320usize;
    let val_w = 130usize;
    let pad = 8usize;
    let svg_w = label_w + pad + bar_area + pad + val_w;
    let svg_h = entries.len() * row_h + 4;

    let mut svg = String::new();
    svg.push_str(&format!(
        "<svg width=\"{svg_w}\" height=\"{svg_h}\" xmlns=\"http://www.w3.org/2000/svg\" style=\"width:100%;max-width:{svg_w}px;display:block\">"
    ));
    for (i, (label, val)) in entries.iter().enumerate() {
        let y = i * row_h;
        let bar_len = (*val as f64 / max_val as f64 * bar_area as f64) as usize;
        let label_trim = if label.len() > 40 {
            format!("\u{2026}{}", &label[label.len().saturating_sub(39)..])
        } else {
            label.to_string()
        };
        let val_label = format!("{} ({})", fmt_bytes(*val), pct(*val, total));
        // label (right-aligned)
        svg.push_str(&format!(
            "<text x=\"{label_w}\" y=\"{}\" text-anchor=\"end\" font-size=\"11\" fill=\"#374151\" font-family=\"sans-serif\">{}</text>",
            y + 15, he(&label_trim),
        ));
        // bar
        svg.push_str(&format!(
            "<rect x=\"{}\" y=\"{}\" width=\"{bar_len}\" height=\"18\" rx=\"2\" fill=\"#6366f1\" opacity=\"0.8\"/>",
            label_w + pad, y + 1,
        ));
        // value text
        svg.push_str(&format!(
            "<text x=\"{}\" y=\"{}\" font-size=\"11\" fill=\"#6b7280\" font-family=\"sans-serif\">{}</text>",
            label_w + pad + bar_len + 6, y + 15, he(&val_label),
        ));
    }
    svg.push_str("</svg>");
    svg
}

// ── Treemap JS data ───────────────────────────────────────────────────────────

fn treemap_data_js(out: &AnalysisOutput) -> String {
    let mut s = String::from("const TREEMAP=");
    s.push('[');
    for (i, pkg) in out.treemap_data.iter().enumerate() {
        if i > 0 { s.push(','); }
        s.push_str(&format!("{{name:{},value:{},classes:[", js_str(&pkg.name), pkg.retained_bytes));
        for (j, cls) in pkg.classes.iter().enumerate() {
            if j > 0 { s.push(','); }
            s.push_str(&format!(
                "{{name:{},value:{},instances:{}}}",
                js_str(&cls.name), cls.retained_bytes, cls.instance_count,
            ));
        }
        s.push_str("]}");
    }
    s.push_str("];\n");
    s.push_str(&format!("const TOTAL_BYTES={};\n", out.total_shallow_bytes));
    s
}

// ── Section: Overview ─────────────────────────────────────────────────────────

fn section_overview(out: &AnalysisOutput) -> String {
    let unr_warn = if out.unreachable_count > 0 { " warn" } else { "" };
    let fin_warn  = if out.finalizer_queue_depth > 10 { " warn" } else { "" };
    let mut s = String::new();
    s.push_str("<section id=\"overview\">\n<h2>Heap Overview</h2>\n<div class=\"cards\">\n");
    let cards = [
        ("Total heap (shallow)", fmt_bytes(out.total_shallow_bytes), format!("{} objects", out.total_objects), ""),
        ("Retained (reachable)",  fmt_bytes(out.retained_heap_bytes), format!("{} GC roots", out.gc_roots), ""),
        ("Classes",               out.total_classes.to_string(), String::new(), ""),
        ("Unreachable (garbage)", fmt_bytes(out.unreachable_shallow), format!("{} objects", out.unreachable_count), unr_warn),
        ("Finalizer queue",       out.finalizer_queue_depth.to_string(), "pending finalization".to_string(), fin_warn),
        ("Soft / Weak / Phantom", format!("{} / {} / {}", out.soft_ref_count, out.weak_ref_count, out.phantom_ref_count), "reference counts".to_string(), ""),
    ];
    for (label, value, sub, extra_class) in &cards {
        s.push_str(&format!(
            "<div class=\"card{extra_class}\"><div class=\"card-label\">{label}</div><div class=\"card-value\">{}</div>",
            he(value),
        ));
        if !sub.is_empty() {
            s.push_str(&format!("<div class=\"card-sub\">{}</div>", he(sub)));
        }
        s.push_str("</div>\n");
    }
    s.push_str("</div>\n</section>");
    s
}

// ── Section: GC Pressure ──────────────────────────────────────────────────────

fn section_gc_pressure(out: &AnalysisOutput) -> String {
    let fin_class  = if out.finalizer_queue_depth > 20 { "danger" } else if out.finalizer_queue_depth > 5 { "warn" } else { "" };
    let unr_class  = if out.unreachable_count > 10_000 { "danger" } else if out.unreachable_count > 1_000 { "warn" } else { "" };

    let mut s = String::new();
    s.push_str("<section id=\"gc-pressure\">\n<h2>GC Pressure</h2>\n<div class=\"gc-grid\">\n");

    // Reference statistics box
    s.push_str("<div class=\"gc-box\"><h3>Reference statistics</h3>\n");
    let refs = [
        ("java.lang.ref.Finalizer",        out.finalizer_queue_depth, fin_class),
        ("java.lang.ref.SoftReference",    out.soft_ref_count,        ""),
        ("java.lang.ref.WeakReference",    out.weak_ref_count,        ""),
        ("java.lang.ref.PhantomReference", out.phantom_ref_count,     ""),
    ];
    for (name, count, cls) in &refs {
        s.push_str(&format!(
            "<div class=\"gc-row\"><span class=\"gc-key\">{}</span><span class=\"gc-val {}\">{count}</span></div>\n",
            he(name), cls,
        ));
    }
    s.push_str("</div>\n");

    // Unreachable box
    s.push_str("<div class=\"gc-box\"><h3>Unreachable objects</h3>\n");
    let unr_rows = [
        ("Object count", format!("<span class=\"gc-val {unr_class}\">{}</span>", out.unreachable_count)),
        ("Shallow size",  format!("<span class=\"gc-val\">{}</span>", he(&fmt_bytes(out.unreachable_shallow)))),
        ("% of total heap", format!("<span class=\"gc-val\">{}</span>", pct(out.unreachable_shallow, out.total_shallow_bytes))),
    ];
    for (k, v) in &unr_rows {
        s.push_str(&format!("<div class=\"gc-row\"><span class=\"gc-key\">{k}</span>{v}</div>\n"));
    }
    s.push_str("</div>\n</div>\n</section>");
    s
}

// ── Section: Leak Suspects ────────────────────────────────────────────────────

fn section_leak_suspects(out: &AnalysisOutput) -> String {
    let mut s = String::new();
    s.push_str("<section id=\"leak-suspects\">\n<h2>Leak Suspects</h2>\n");
    if out.leak_suspects.is_empty() {
        s.push_str("<p style=\"color:#64748b;font-size:.875rem\">No single class retains \u{2265} 1% of the heap. No obvious suspects.</p>\n");
    } else {
        s.push_str("<div class=\"suspects\">\n");
        for (i, sus) in out.leak_suspects.iter().enumerate() {
            s.push_str(&format!(
                "<div class=\"suspect\"><div class=\"suspect-n\">Problem Suspect {}</div>\
                 <div class=\"suspect-class\">{}</div>\
                 <div class=\"suspect-stats\">\
                   <span>{:.1}% of heap</span>\
                   <span>{} retained</span>\
                   <span>{} instances</span>\
                   <span>{} avg/inst</span>\
                 </div>\
                 <div class=\"suspect-pattern\">{}</div></div>\n",
                i + 1,
                he(&sus.class_name),
                sus.pct_of_heap,
                he(&fmt_bytes(sus.total_retained_bytes)),
                sus.instance_count,
                he(&fmt_bytes(sus.avg_retained_bytes)),
                he(sus.pattern),
            ));
        }
        s.push_str("</div>\n");
    }
    s.push_str("</section>");
    s
}

// ── Section: Treemap ──────────────────────────────────────────────────────────

fn section_treemap() -> String {
    let mut s = String::new();
    s.push_str("<section id=\"treemap\">\n<h2>Retained Heap Treemap</h2>\n");
    s.push_str("<div class=\"treemap-wrap\">\n");
    s.push_str("<div class=\"treemap-hint\">Click a package to drill into its classes. Click the header bar to go back.</div>\n");
    s.push_str("<canvas id=\"tm-canvas\"></canvas>\n");
    s.push_str("</div>\n<div id=\"tm-tooltip\"></div>\n</section>");
    s
}

// ── Section: Class Histogram ──────────────────────────────────────────────────

fn section_histogram(out: &AnalysisOutput) -> String {
    let total = out.total_shallow_bytes;
    let alloc: Vec<(&str, u64)> = out.top_allocated.iter()
        .map(|e| (e.class_name.as_str(), e.total_shallow_bytes))
        .collect();
    let retained: Vec<(&str, u64)> = out.retained_by_class.iter()
        .map(|e| (e.class_name.as_str(), e.total_retained_bytes))
        .collect();

    let mut s = String::new();
    s.push_str("<section id=\"histogram\">\n<h2>Class Histogram</h2>\n");
    s.push_str("<div class=\"chart-wrap\">\n<div class=\"chart-title\">Top classes by total allocation (shallow bytes)</div>\n");
    s.push_str(&bar_chart(&alloc, total));
    s.push_str("\n</div>\n<div class=\"chart-wrap\" style=\"margin-top:1rem\">\n");
    s.push_str("<div class=\"chart-title\">Top classes by retained heap</div>\n");
    s.push_str(&bar_chart(&retained, total));
    s.push_str("\n</div>\n</section>");
    s
}

// ── Section: Package Summary ──────────────────────────────────────────────────

fn section_packages(out: &AnalysisOutput) -> String {
    let mut s = String::new();
    s.push_str("<section id=\"packages\">\n<h2>Package Summary</h2>\n<div class=\"tbl-wrap\">\n<table>\n");
    s.push_str("<thead><tr><th>Package</th><th class=\"r\">Retained</th><th class=\"r\">% heap</th><th class=\"r\">Shallow</th><th class=\"r\">Classes</th><th class=\"r\">Instances</th></tr></thead>\n<tbody>\n");
    for e in &out.package_summary {
        s.push_str(&format!(
            "<tr><td class=\"mono\">{}</td><td class=\"r\">{}</td><td class=\"r\">{}</td><td class=\"r\">{}</td><td class=\"r\">{}</td><td class=\"r\">{}</td></tr>\n",
            he(&e.package),
            he(&fmt_bytes(e.total_retained_bytes)),
            pct(e.total_retained_bytes, out.total_shallow_bytes),
            he(&fmt_bytes(e.total_shallow_bytes)),
            e.class_count,
            e.instance_count,
        ));
    }
    s.push_str("</tbody>\n</table>\n</div>\n</section>");
    s
}

// ── Public entry point ────────────────────────────────────────────────────────

pub fn render(out: &AnalysisOutput) -> String {
    let mut html = String::with_capacity(512 * 1024);

    html.push_str("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n");
    html.push_str("<meta charset=\"utf-8\">\n");
    html.push_str("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n");
    html.push_str("<title>minprof \u{2014} heap analysis</title>\n");
    html.push_str("<style>");
    html.push_str(CSS);
    html.push_str("</style>\n</head>\n<body>\n");

    // Header
    html.push_str("<header>\n<h1>minprof \u{2014} heap analysis</h1>\n<div class=\"sub\">");
    html.push_str(&format!(
        "{} objects &middot; {} classes &middot; {} GC roots &middot; {}",
        out.total_objects, out.total_classes, out.gc_roots,
        he(&fmt_bytes(out.total_shallow_bytes)),
    ));
    html.push_str("</div>\n</header>\n");

    // Nav
    html.push_str("<nav>\n");
    for (id, label) in &[
        ("overview",     "Overview"),
        ("gc-pressure",  "GC Pressure"),
        ("leak-suspects","Leak Suspects"),
        ("treemap",      "Treemap"),
        ("histogram",    "Histogram"),
        ("packages",     "Packages"),
    ] {
        html.push_str(&format!("<a href=\"#{id}\">{label}</a>\n"));
    }
    html.push_str("</nav>\n<main>\n");

    html.push_str(&section_overview(out));
    html.push_str("\n");
    html.push_str(&section_gc_pressure(out));
    html.push_str("\n");
    html.push_str(&section_leak_suspects(out));
    html.push_str("\n");
    html.push_str(&section_treemap());
    html.push_str("\n");
    html.push_str(&section_histogram(out));
    html.push_str("\n");
    html.push_str(&section_packages(out));

    html.push_str("\n</main>\n<script>\n");
    html.push_str(&treemap_data_js(out));
    html.push_str(JS_STATIC);
    html.push_str("</script>\n</body>\n</html>\n");

    html
}
