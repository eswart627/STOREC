let progressSource = null;

let selectedFile = null;

/* =========================
   LOAD FILE LIST
========================= */

async function loadFiles() {
  const refreshBtn = document.getElementById("refreshBtn");

  const container = document.getElementById("fileList");

  /* spin button */

  const icon = document.getElementById("refreshIcon");

  if (icon) {
    icon.classList.add("spin");

    setTimeout(() => icon.classList.remove("spin"), 600);
  }

  /* subtle fade */
  container.style.opacity = "0.4";

  try {
    const res = await fetch("/api/files");

    const data = await res.json();

    container.innerHTML = "";

    if (!data.files || data.files.length === 0) {
      container.innerHTML = "<div style='color:#666'>No files available</div>";
    } else {
      data.files.forEach((fileName) => {
        const row = document.createElement("div");

        row.textContent = fileName;

        row.onclick = function () {
          selectFile(fileName, row);
        };

        container.appendChild(row);
      });
    }

    /* flick animation */

    container.classList.add("file-refresh");

    setTimeout(() => container.classList.remove("file-refresh"), 250);

    /* optional notification */

    notify("File list refreshed", "info");
  } catch (err) {
    notify("Refresh failed", "error");
  } finally {
    container.style.opacity = "1";
  }
}
/* =========================
   FILE SELECTION
========================= */

function selectFile(fileName, element) {
  selectedFile = fileName;

  document
    .querySelectorAll("#fileList div")
    .forEach((row) => row.classList.remove("file-selected"));

  element.classList.add("file-selected");
}

/* =========================
   PROGRESS STREAM
========================= */

function listenProgress() {
  if (progressSource) progressSource.close();

  progressSource = new EventSource("/api/progress");

  progressSource.onmessage = function (event) {
    const consoleBox = document.getElementById("console");

    consoleBox.innerHTML += event.data + "<br>";

    consoleBox.scrollTop = consoleBox.scrollHeight;
  };
}

/* =========================
   UPLOAD FILE
========================= */

async function uploadFile() {
  const selectedUpload = document.getElementById("fileInput").files[0];

  if (!selectedUpload) {
    notify("Select a file");

    return;
  }

  const modeElement = document.querySelector('input[name="mode"]:checked');

  const mode = modeElement ? modeElement.value : "parallel";

  const formData = new FormData();

  formData.append("file", selectedUpload);

  formData.append("mode", mode);

  const consoleBox = document.getElementById("console");

  consoleBox.innerHTML = "";

  listenProgress();

  await fetch("/api/upload", {
    method: "POST",
    body: formData,
  });

  loadFiles();
}

/* =========================
   DOWNLOAD SELECTED FILE
========================= */

async function readSelectedFile() {
  if (!selectedFile) {
    notify("Select a file first");

    return;
  }

  const res = await fetch("/api/read", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      file_name: selectedFile,
    }),
  });

  if (!res.ok) {
    const err = await res.text();

    notify("Download failed: " + err);

    return;
  }

  const data = await res.json();

  notify("File downloaded:\n" + data.path);
}

/* =========================
   DELETE SELECTED FILE
========================= */

async function deleteSelectedFile() {
  if (!selectedFile) {
    notify("Select a file first");

    return;
  }

  const confirmDelete = confirm("Delete file: " + selectedFile + " ?");

  if (!confirmDelete) return;

  const res = await fetch("/api/delete", {
    method: "DELETE",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      file_name: selectedFile,
    }),
  });

  if (!res.ok) {
    const err = await res.text();

    notify("Delete failed: " + err);

    return;
  }

  selectedFile = null;

  loadFiles();
}

function notify(message, type = "info") {
  const container = document.getElementById("notificationContainer");

  const note = document.createElement("div");

  note.className = "notification " + type;

  note.textContent = message;

  container.appendChild(note);

  setTimeout(() => note.remove(), 3000);
}

/* =========================
   INITIAL LOAD
========================= */

window.onload = function () {
  loadFiles();
};
