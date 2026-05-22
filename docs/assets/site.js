/* =========================================================================
   Ariadne docs — small runtime helpers.
   - Anchored headings (click-to-link)
   - Copy buttons on code blocks
   - Active state for header nav based on data-section
   - Lightweight syntax highlighting for scala/bash/xml/sql/json
   ========================================================================= */
(function () {
  'use strict';

  function ready(fn) {
    if (document.readyState !== 'loading') fn();
    else document.addEventListener('DOMContentLoaded', fn);
  }

  /* --- Active header nav -------------------------------------------------- */
  function markActiveNav() {
    var section = document.body.dataset.section;
    if (!section) return;
    document.querySelectorAll('.primary-nav a[data-section]').forEach(function (a) {
      if (a.dataset.section === section) a.classList.add('active');
    });
  }

  /* --- Active sidebar link ------------------------------------------------ */
  function markActiveSidebar() {
    var page = document.body.dataset.page;
    if (!page) return;
    document.querySelectorAll('.sidebar a[data-page]').forEach(function (a) {
      if (a.dataset.page === page) a.classList.add('active');
    });
  }

  /* --- Heading anchors ---------------------------------------------------- */
  function addAnchors() {
    document.querySelectorAll('main.content h2[id], main.content h3[id], main.content h4[id]')
      .forEach(function (h) {
        var a = document.createElement('a');
        a.className = 'anchor';
        a.href = '#' + h.id;
        a.textContent = '#';
        a.setAttribute('aria-label', 'Link to ' + h.textContent);
        h.appendChild(a);
      });
  }

  /* --- Copy buttons on code blocks --------------------------------------- */
  function addCopyButtons() {
    document.querySelectorAll('pre').forEach(function (pre) {
      var wrap = document.createElement('div');
      wrap.className = 'code-block';
      pre.parentNode.insertBefore(wrap, pre);
      wrap.appendChild(pre);

      var btn = document.createElement('button');
      btn.className = 'copy-btn';
      btn.type = 'button';
      btn.textContent = 'Copy';
      btn.addEventListener('click', function () {
        var code = pre.querySelector('code') || pre;
        var text = code.innerText;
        navigator.clipboard.writeText(text).then(function () {
          btn.textContent = 'Copied';
          btn.classList.add('copied');
          setTimeout(function () {
            btn.textContent = 'Copy';
            btn.classList.remove('copied');
          }, 1400);
        });
      });
      wrap.appendChild(btn);
    });
  }

  /* --- Lightweight syntax highlighting ----------------------------------- */
  // Tiny tokenizer keyed by language. Not a full highlighter; covers common
  // patterns nicely without an external dependency.
  var LANGS = {
    scala: {
      kw: /\b(abstract|case|catch|class|def|do|else|extends|false|final|finally|for|forSome|if|implicit|import|lazy|match|new|null|object|override|package|private|protected|return|sealed|super|this|throw|trait|true|try|type|val|var|while|with|yield)\b/g,
      typ: /\b([A-Z][A-Za-z0-9_]*)\b/g,
      fn: /\b([a-z_][A-Za-z0-9_]*)(?=\s*[\(\[])/g
    },
    bash: {
      kw: /\b(if|then|else|elif|fi|case|esac|for|while|do|done|in|function|return|export|local|cd|echo|exit|sudo|mvn|docker|cp|mkdir|touch|grep|cat|set|source)\b/g,
      typ: /(^|\s)(-[A-Za-z]+|--[A-Za-z-]+)/g,
      fn: null
    },
    xml: {
      // handled specially below
    },
    sql: {
      kw: /\b(SELECT|FROM|WHERE|JOIN|INNER|LEFT|RIGHT|FULL|OUTER|ON|AS|AND|OR|NOT|IN|GROUP|BY|ORDER|HAVING|LIMIT|INSERT|INTO|VALUES|UPDATE|SET|DELETE|CREATE|TABLE|DROP|SHOW|TABLES|DESCRIBE|IF|EXISTS)\b/gi,
      typ: null,
      fn: null
    },
    json: {}
  };

  function escapeHtml(s) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  // Tokenize ordering: comments, strings, numbers, then keywords/types/funcs.
  function highlightGeneric(src, spec) {
    // Use placeholders to protect tokenized regions.
    var stash = [];
    function stashTok(cls, text) {
      var i = stash.length;
      stash.push('<span class="tok-' + cls + '">' + escapeHtml(text) + '</span>');
      return '\u0000' + i + '\u0000';
    }
    // Strings (double, single, triple)
    src = src.replace(/"""[\s\S]*?"""|"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'/g, function (m) {
      return stashTok('str', m);
    });
    // Line comments
    src = src.replace(/\/\/[^\n]*/g, function (m) { return stashTok('com', m); });
    // Hash comments (bash)
    src = src.replace(/(^|\s)#[^\n]*/g, function (m, p) { return p + stashTok('com', m.slice(p.length)); });
    // Block comments
    src = src.replace(/\/\*[\s\S]*?\*\//g, function (m) { return stashTok('com', m); });
    // Numbers
    src = src.replace(/\b\d+(?:\.\d+)?\b/g, function (m) { return stashTok('num', m); });

    // Escape everything else
    src = escapeHtml(src);

    // Keywords / types / functions (operate on escaped text)
    if (spec.kw) src = src.replace(spec.kw, '<span class="tok-kw">$&</span>');
    if (spec.typ) src = src.replace(spec.typ, function (m, g1, g2) {
      if (g2 !== undefined) return g1 + '<span class="tok-typ">' + g2 + '</span>';
      return '<span class="tok-typ">' + m + '</span>';
    });
    if (spec.fn) src = src.replace(spec.fn, '<span class="tok-fn">$1</span>');

    // Restore stashed tokens
    src = src.replace(/\u0000(\d+)\u0000/g, function (_, i) { return stash[+i]; });
    return src;
  }

  function highlightXml(src) {
    return escapeHtml(src)
      .replace(/(&lt;!--[\s\S]*?--&gt;)/g, '<span class="tok-com">$1</span>')
      .replace(/(&lt;\/?)([A-Za-z][A-Za-z0-9-]*)/g,
        '<span class="tok-pun">$1</span><span class="tok-kw">$2</span>')
      .replace(/(\s)([A-Za-z-]+)=(&quot;[^&]*&quot;)/g,
        '$1<span class="tok-typ">$2</span>=<span class="tok-str">$3</span>')
      .replace(/(\/?&gt;)/g, '<span class="tok-pun">$1</span>');
  }

  function highlightJson(src) {
    return escapeHtml(src)
      .replace(/(&quot;[^&]*?&quot;)(\s*:)/g, '<span class="tok-typ">$1</span>$2')
      .replace(/:\s*(&quot;[^&]*?&quot;)/g, ': <span class="tok-str">$1</span>')
      .replace(/\b(true|false|null)\b/g, '<span class="tok-kw">$1</span>')
      .replace(/\b(-?\d+(?:\.\d+)?)\b/g, '<span class="tok-num">$1</span>');
  }

  function highlightAll() {
    document.querySelectorAll('pre > code[class*="language-"]').forEach(function (code) {
      var cls = code.className.match(/language-(\w+)/);
      if (!cls) return;
      var lang = cls[1];
      var src = code.textContent;
      var out;
      if (lang === 'xml' || lang === 'html') out = highlightXml(src);
      else if (lang === 'json') out = highlightJson(src);
      else if (LANGS[lang]) out = highlightGeneric(src, LANGS[lang]);
      else out = escapeHtml(src);
      code.innerHTML = out;
    });
  }

  ready(function () {
    markActiveNav();
    markActiveSidebar();
    addAnchors();
    highlightAll();
    addCopyButtons();
  });
})();
