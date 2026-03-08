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

  function openActiveRow(linkIndex) {
    var rows = getRows();
    if (activeRow < 0 || activeRow >= rows.length) return;
    var links = rows[activeRow].querySelectorAll('a');
    if (links.length === 0) return;
    var idx = (linkIndex !== undefined && linkIndex < links.length) ? linkIndex : 0;
    links[idx].click();
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
    // Always allow Escape to close help / exit delete mode
    if (e.key === 'Escape') {
      var overlay = document.getElementById('help-overlay');
      if (overlay && overlay.classList.contains('visible')) {
        hideHelp();
        e.preventDefault();
        return;
      }
      if (isDeleteMode()) {
        exitDeleteMode();
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
          openActiveRow(0);
        }
        break;

      case 'o':
        // Open messages for active row (second link, or first if only one)
        if (activeRow >= 0) {
          e.preventDefault();
          openActiveRow(1);
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

      case 'Backspace':
        // Navigate back via breadcrumb link
        e.preventDefault();
        var backLink = document.querySelector('.breadcrumb a');
        if (backLink) {
          backLink.click();
        }
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

      case 'd':
        // Enter delete mode
        if (!isDeleteMode()) {
          e.preventDefault();
          enterDeleteMode();
        }
        break;

      case ' ':
        // Toggle selection on active row (delete mode only)
        if (isDeleteMode() && activeRow >= 0) {
          e.preventDefault();
          toggleActiveRowCheckbox();
        }
        break;

      case 'x':
        // Clear selection (delete mode)
        if (isDeleteMode()) {
          var boxes = document.querySelectorAll('.msg-checkbox');
          for (var i = 0; i < boxes.length; i++) boxes[i].checked = false;
          var selectAll = document.getElementById('select-all');
          if (selectAll) { selectAll.checked = false; selectAll.indeterminate = false; }
          updateSelectionInfo();
        }
        break;

      case 'A':
        // Select all (delete mode)
        if (isDeleteMode()) {
          e.preventDefault();
          selectAllMessages();
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

  // Theme toggle: cycles auto → dark → light → auto
  function setupThemeToggle() {
    var btn = document.getElementById('theme-toggle');
    if (!btn) return;

    var saved = localStorage.getItem('msgvault-theme') || 'auto';
    applyTheme(saved);

    btn.addEventListener('click', function () {
      var current = localStorage.getItem('msgvault-theme') || 'auto';
      var next = current === 'auto' ? 'dark' : current === 'dark' ? 'light' : 'auto';
      localStorage.setItem('msgvault-theme', next);
      applyTheme(next);
    });
  }

  function applyTheme(theme) {
    var root = document.documentElement;
    var btn = document.getElementById('theme-toggle');
    if (theme === 'dark') {
      root.setAttribute('data-theme', 'dark');
      if (btn) btn.textContent = '\u263E';  // moon
    } else if (theme === 'light') {
      root.setAttribute('data-theme', 'light');
      if (btn) btn.textContent = '\u2600';  // sun
    } else {
      root.removeAttribute('data-theme');
      if (btn) btn.textContent = '\u25D1';  // half circle (auto)
    }
  }

  // Delete mode — toggled by 'd' key
  var deleteMode = false;

  function isDeleteMode() {
    return deleteMode;
  }

  function enterDeleteMode() {
    if (!document.querySelector('.msg-checkbox')) return; // no checkboxes on page
    deleteMode = true;
    document.body.classList.add('delete-mode');
    updateSelectionInfo();
  }

  window.exitDeleteMode = function () {
    deleteMode = false;
    document.body.classList.remove('delete-mode');
    // Uncheck everything
    var boxes = document.querySelectorAll('.msg-checkbox');
    for (var i = 0; i < boxes.length; i++) boxes[i].checked = false;
    var selectAll = document.getElementById('select-all');
    if (selectAll) { selectAll.checked = false; selectAll.indeterminate = false; }
    updateSelectionInfo();
  };

  window.selectAllMessages = function () {
    var boxes = document.querySelectorAll('.msg-checkbox');
    for (var i = 0; i < boxes.length; i++) boxes[i].checked = true;
    var selectAll = document.getElementById('select-all');
    if (selectAll) { selectAll.checked = true; selectAll.indeterminate = false; }
    updateSelectionInfo();
  };

  function updateSelectionInfo() {
    var info = document.getElementById('sel-info');
    var submit = document.getElementById('sel-submit');
    if (!info) return;
    var checked = document.querySelectorAll('.msg-checkbox:checked');
    var total = document.querySelectorAll('.msg-checkbox');
    if (checked.length === 0) {
      info.textContent = 'Select messages to stage for deletion';
      if (submit) { submit.disabled = true; submit.textContent = 'Stage for Deletion'; }
    } else {
      info.textContent = checked.length + ' of ' + total.length + ' selected';
      if (submit) { submit.disabled = false; submit.textContent = 'Stage ' + checked.length + ' for Deletion'; }
    }
    // Update select-all checkbox state
    var selectAll = document.getElementById('select-all');
    if (selectAll) {
      selectAll.checked = total.length > 0 && checked.length === total.length;
      selectAll.indeterminate = checked.length > 0 && checked.length < total.length;
    }
  }

  function setupSelection() {
    document.addEventListener('change', function (e) {
      if (e.target.classList.contains('msg-checkbox') || e.target.id === 'select-all') {
        if (e.target.id === 'select-all') {
          var boxes = document.querySelectorAll('.msg-checkbox');
          for (var i = 0; i < boxes.length; i++) boxes[i].checked = e.target.checked;
        }
        updateSelectionInfo();
      }
    });
  }

  function toggleActiveRowCheckbox() {
    if (!isDeleteMode()) return;
    var rows = getRows();
    if (activeRow < 0 || activeRow >= rows.length) return;
    var cb = rows[activeRow].querySelector('.msg-checkbox');
    if (cb) {
      cb.checked = !cb.checked;
      updateSelectionInfo();
    }
  }

  // Reset active row on page load
  activeRow = -1;
  setupSearchLoading();
  setupThemeToggle();
  setupSelection();
})();
