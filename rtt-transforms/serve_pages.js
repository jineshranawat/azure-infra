const http = require('http');
const fs = require('fs');
const path = require('path');
const root = process.cwd();
const PORT = process.env.PORT || 8080;
const types = {'.html':'text/html; charset=utf-8','.js':'text/javascript','.css':'text/css','.json':'application/json','.svg':'image/svg+xml','.md':'text/plain; charset=utf-8','.png':'image/png','.ico':'image/x-icon'};
const links = [
  ['Engine Command Centre (owner manual)', '/docs/engine_command_centre.html'],
  ['Pipeline Explainer (theory + graphs)', '/docs/pipeline_explainer.html'],
  ['RTT Command Centre (React app)', '/app/rtt_command_centre.html'],
  ['Docs README', '/docs/README.md'],
  ['Traceability matrix', '/docs/TRACEABILITY.md'],
  ['Deployment runbook', '/docs/DEPLOYMENT.md'],
  ['Platform specs', '/docs/SPECS.md'],
  ['OSDK app README', '/osdk-app/README.md']
];
function indexPage() {
  const items = links.map(function(l){ return '<li><a style="color:#7db4ff" href="' + l[1] + '">' + l[0] + '</a></li>'; }).join('');
  return '<!doctype html><meta charset=utf-8><title>RTT-Programme local</title>' +
    '<body style="font:16px system-ui,Arial;background:#0b1526;color:#e7eef7;max-width:760px;margin:40px auto;padding:0 20px">' +
    '<h1 style="color:#63b3ed">RTT-Programme &mdash; local server</h1>' +
    '<p style="color:#9fb2c9">Node static server. Root: ' + root + '</p><ul style="line-height:2.2">' + items + '</ul></body>';
}
const server = http.createServer(function(req, res) {
  let p = decodeURIComponent((req.url || '/').split('?')[0]);
  if (p === '/') { res.writeHead(200, {'Content-Type':'text/html; charset=utf-8'}); res.end(indexPage()); return; }
  let fp = path.join(root, p);
  if (fp.indexOf(root) !== 0) { res.writeHead(403); res.end('forbidden'); return; }
  fs.stat(fp, function(e, st) {
    if (e) { res.writeHead(404); res.end('Not found: ' + p); return; }
    if (st.isDirectory()) fp = path.join(fp, 'index.html');
    fs.readFile(fp, function(e2, data) {
      if (e2) { res.writeHead(404); res.end('Not found'); return; }
      res.writeHead(200, {'Content-Type': types[path.extname(fp)] || 'application/octet-stream'});
      res.end(data);
    });
  });
});
server.listen(PORT, function(){ console.log('RTT pages serving on http://localhost:' + PORT); });
