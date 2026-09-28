/* Module 1 — shared helpers: theme toggle, SVG builder, self-quiz engine */

(function initTheme() {
  let saved = null;
  try { saved = localStorage.getItem('kafka-viz-theme'); } catch (e) {}
  if (saved) document.documentElement.setAttribute('data-theme', saved);
})();

function toggleTheme() {
  const root = document.documentElement;
  const current = root.getAttribute('data-theme') ||
    (matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
  const next = current === 'dark' ? 'light' : 'dark';
  root.setAttribute('data-theme', next);
  try { localStorage.setItem('kafka-viz-theme', next); } catch (e) {}
}

const SVGNS = 'http://www.w3.org/2000/svg';
function el(tag, attrs, parent) {
  const node = document.createElementNS(SVGNS, tag);
  for (const k in attrs || {}) {
    if (k === 'text') node.textContent = attrs[k];
    else node.setAttribute(k, attrs[k]);
  }
  if (parent) parent.appendChild(node);
  return node;
}

const PCOLORS = ['var(--p0)', 'var(--p1)', 'var(--p2)', 'var(--p3)', 'var(--p4)', 'var(--p5)'];

const sleep = ms => new Promise(r => setTimeout(r, ms));
const reducedMotion = matchMedia('(prefers-reduced-motion: reduce)').matches;

/* Animate an SVG element from (x1,y1) to (x2,y2) via transform. */
function tween(node, x1, y1, x2, y2, ms) {
  ms = reducedMotion ? 1 : (ms || 600);
  return new Promise(resolve => {
    const t0 = performance.now();
    function frame(t) {
      const p = Math.min(1, (t - t0) / ms);
      const e = p < .5 ? 2 * p * p : 1 - Math.pow(-2 * p + 2, 2) / 2;
      node.setAttribute('transform', `translate(${x1 + (x2 - x1) * e},${y1 + (y2 - y1) * e})`);
      if (p < 1) requestAnimationFrame(frame); else resolve();
    }
    requestAnimationFrame(frame);
  });
}

function logLine(logEl, html, highlight) {
  const d = document.createElement('div');
  d.innerHTML = html;
  if (highlight) d.className = 'hl';
  logEl.prepend(d);
  while (logEl.children.length > 40) logEl.lastChild.remove();
}

/*
 * Self-quiz. questions: [{ q, options: [...], answer: index, explain }]
 */
function renderQuiz(container, questions) {
  const root = typeof container === 'string' ? document.getElementById(container) : container;
  root.classList.add('quiz');
  let answered = 0, correct = 0;
  const score = document.createElement('div');
  score.className = 'quiz-score';

  function updateScore() {
    const done = answered === questions.length;
    score.innerHTML = `<span><b>${correct}</b> / ${questions.length} correct` +
      (done ? ` — ${correct === questions.length ? 'excellent, you’ve got this topic.' :
        correct >= questions.length - 1 ? 'nearly there. Review the explanation you missed.' :
        'revisit the visualization above and try again.'}` : ` · ${questions.length - answered} left`) +
      `</span>`;
    const reset = document.createElement('button');
    reset.textContent = 'Reset quiz';
    reset.onclick = build;
    score.appendChild(reset);
  }

  function build() {
    answered = 0; correct = 0;
    root.innerHTML = '';
    questions.forEach((item, qi) => {
      const box = document.createElement('div');
      box.className = 'q';
      box.innerHTML = `<div class="q-title">${qi + 1}. ${item.q}</div>`;
      const opts = document.createElement('div');
      opts.className = 'q-opts';
      const explain = document.createElement('div');
      explain.className = 'q-explain';
      item.options.forEach((text, oi) => {
        const b = document.createElement('button');
        b.className = 'q-opt';
        b.innerHTML = `<span class="k">${String.fromCharCode(65 + oi)}</span><span>${text}</span>`;
        b.onclick = () => {
          const buttons = opts.querySelectorAll('button');
          buttons.forEach(x => x.disabled = true);
          buttons[item.answer].classList.add('correct');
          const ok = oi === item.answer;
          if (!ok) b.classList.add('wrong');
          answered++; if (ok) correct++;
          explain.innerHTML = `<b>${ok ? 'Correct.' : 'Not quite.'}</b> ${item.explain}`;
          explain.classList.add('show');
          updateScore();
        };
        opts.appendChild(b);
      });
      box.appendChild(opts);
      box.appendChild(explain);
      root.appendChild(box);
    });
    root.appendChild(score);
    updateScore();
  }
  build();
}
