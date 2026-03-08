// Keyboard shortcuts for msgvault web UI
// Mirrors TUI bindings: j/k navigation, / search, ? help
(function () {
  'use strict';

  var activeRow = -1;

  function getRows() {
    var table = document.querySelector('.data-table tbody');
    return table ? table.querySelectorAll('tr') : [];
  }

  function clearActive() {
    var rows = getRows();
    for (var i = 0; i < rows.length; i++) {
      rows[i].classList.remove('kb-active');
    }
  }

  function setActive(index) {
    var rows = getRows();
    if (rows.length === 0) return;
    if (index < 0) index = 0;
    if (index >= rows.length) index = rows.length - 1;
    clearActive();
    activeRow = index;
    rows[activeRow].classList.add('kb-active');
    rows[activeRow].scrollIntoView({ block: 'nearest' });
  }

  function openActiveRow() {
    var rows = getRows();
    if (activeRow < 0 || activeRow >= rows.length) return;
    var link = rows[activeRow].querySelector('a');
    if (link) link.click();
  }

  function isInputFocused() {
    var el = document.activeElement;
    if (!el) return false;
    var tag = el.tagName.toLowerCase();
    return tag === 'input' || tag === 'textarea' || tag === 'select' || el.isContentEditable;
  }

  // Help overlay
  function toggleHelp() {
    var overlay = document.getElementById('help-overlay');
    if (overlay) overlay.classList.toggle('visible');
  }

  function hideHelp() {
    var overlay = document.getElementById('help-overlay');
    if (overlay) overlay.classList.remove('visible');
  }

  // Search form loading state
  function setupSearchLoading() {
    var form = document.querySelector('.search-form');
    if (!form) return;
    form.addEventListener('submit', function () {
      var btn = form.querySelector('.search-btn');
      if (!btn) return;
      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Searching\u2026';
    });
  }

  document.addEventListener('keydown', function (e) {
    // Always allow Escape to close help
    if (e.key === 'Escape') {
      var overlay = document.getElementById('help-overlay');
      if (overlay && overlay.classList.contains('visible')) {
        hideHelp();
        e.preventDefault();
        return;
      }
      // Escape also blurs search input
      if (isInputFocused()) {
        document.activeElement.blur();
        e.preventDefault();
        return;
      }
    }

    // Don't capture shortcuts when typing in inputs
    if (isInputFocused()) return;

    switch (e.key) {
      case '/':
        e.preventDefault();
        // If on search page, focus the input
        var searchInput = document.querySelector('.search-input');
        if (searchInput) {
          searchInput.focus();
          searchInput.select();
        } else {
          // Navigate to search page
          window.location.href = '/search';
        }
        break;

      case '?':
        e.preventDefault();
        toggleHelp();
        break;

      case 'j':
      case 'ArrowDown':
        e.preventDefault();
        setActive(activeRow + 1);
        break;

      case 'k':
      case 'ArrowUp':
        e.preventDefault();
        setActive(activeRow - 1);
        break;

      case 'Enter':
        if (activeRow >= 0) {
          e.preventDefault();
          openActiveRow();
        }
        break;

      case 'g':
        // gg = go to top (first row)
        e.preventDefault();
        setActive(0);
        break;

      case 'G':
        e.preventDefault();
        var rows = getRows();
        setActive(rows.length - 1);
        break;

      case 'H':
        // Go home (dashboard)
        window.location.href = '/';
        break;

      case 'B':
        // Go to browse
        window.location.href = '/browse';
        break;

      case 'n':
        // Next page
        var nextLink = document.querySelector('.pagination a:last-of-type');
        if (nextLink && nextLink.textContent.trim() === 'Next') {
          nextLink.click();
        }
        break;

      case 'p':
        // Previous page
        var prevLink = document.querySelector('.pagination a:first-of-type');
        if (prevLink && prevLink.textContent.trim() === 'Prev') {
          prevLink.click();
        }
        break;
    }
  });

  // Click on help overlay backdrop to close
  document.addEventListener('click', function (e) {
    var overlay = document.getElementById('help-overlay');
    if (overlay && e.target === overlay) {
      hideHelp();
    }
  });

  // Reset active row on page load
  activeRow = -1;
  setupSearchLoading();
})();
