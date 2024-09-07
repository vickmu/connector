const { app, BrowserWindow } = require('electron');
const { exec } = require('child_process');
const path = require('path');

let pythonProcess = null;

function createWindow() {
  const win = new BrowserWindow({
    width: 800,
    height: 600,
    webPreferences: {
      nodeIntegration: true
    }
  });

  win.loadFile('src/index.html');

  // Start the Python backend
  const script = path.join(__dirname, '..', 'backend', 'app.py');
  pythonProcess = exec(`python ${script}`, (error, stdout, stderr) => {
    if (error) {
      console.error(`Error: ${error}`);
    }
    console.log(`Python Output: ${stdout}`);
  });
}

app.on('ready', createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('quit', () => {
  if (pythonProcess) pythonProcess.kill();  // Ensure the Python process stops when Electron quits
});
