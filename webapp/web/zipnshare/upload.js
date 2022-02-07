window.onload = function() {

	var elemForm1 = document.getElementById("form1");
	var elemDivFileDrop = document.getElementById("divFileDrop");
	var elemInputFileSelect = document.getElementById("inputFileSelect");
	var elemDivFilesToUpload = document.getElementById("divFilesToUpload");
	var elemInputOwnerKey = document.getElementById("inputOwnerKey");
	var elemProgressUpload = document.getElementById("progressUpload");
	var elemButtonUpload = document.getElementById("buttonUpload");
	var elemButtonCancel = document.getElementById("buttonCancel");

	function setButtons(upload,cancel) {
		elemButtonUpload.disabled = !upload;
		elemButtonCancel.disabled = !cancel;
	}

	// init buttons
	setButtons(true,false); // upload,cancel

	// [MEMO] to prevent submit on Enter key
	elemForm1.onkeypress = function(e) {
		var key = e.charCode || e.keyCode || 0;     
		if (key == 13) {
			e.preventDefault();
		}
	}

	// files to upload key: fileName, val:blobObject
	var filesToUpload = {};

	function updateFilesToUpload() {
		elemDivFilesToUpload.innerHTML = "";
		if (Object.keys(filesToUpload).length === 0) {
			var item = document.createElement("div");
			item.innerText = "no files selected";;
			elemDivFilesToUpload.appendChild(item);
		} else {
			for(var filename in filesToUpload) {
				var item = document.createElement("div");
				var label = document.createElement("span");
				label.innerText = filename;
				var delButton = document.createElement("button");
				delButton.innerText = 'delete';
				delButton.setAttribute("filename",filename);
				delButton.onclick = function(e) {
					var target = e.target || e.srcElement;
					var filename1 = target.getAttribute("filename");
					e.preventDefault();
					delete filesToUpload [filename1];
					updateFilesToUpload();
				}
				item.appendChild(label)
				item.appendChild(delButton)
				elemDivFilesToUpload.appendChild(item);
			}
		}
	}
	updateFilesToUpload();

	// https://developer.mozilla.org/en-US/docs/Web/API/HTML_Drag_and_Drop_API/File_drag_and_drop
	elemDivFileDrop.ondragover = function(e) {
		e.preventDefault();
	}
	elemDivFileDrop.ondrop = function(e) {
		e.preventDefault();
		if (e.dataTransfer.items) {
			// Use DataTransferItemList interface to access the file(s)
			for (var i = 0; i < e.dataTransfer.items.length; i++) {
				// If dropped items aren't files, reject them
				if (e.dataTransfer.items[i].kind === 'file') {
					var file = e.dataTransfer.items[i].getAsFile();
					filesToUpload[file.name] = file;
				}
			}
			updateFilesToUpload();
		}

	}

	elemInputFileSelect.onchange = function(e) {
		var files = elemInputFileSelect.files;
		for (var i=0; i < files.length; i++) {
			var file = files[i];
			filesToUpload[file.name] = file;
		}
		elemInputFileSelect.value = null;
		updateFilesToUpload();
	}

	var req;
	async function doPosts() {
		// [TODO] disable more ui controls
		var sessionKey = await new Promise(function (resolve, reject) {
			req = new XMLHttpRequest();
			req.open("POST", "upload");
			req.onload = function (e) {
				if (req.readyState === req.DONE) {
					console.log("result: " + req.responseText);
					if (req.status === 200) {
						resolve(req.responseText);
					} else {
						reject(req.responseText);
					}
				}
			}
			req.onabort = function (e) {
				reject("aborted");
			}
			req.onerror = function (e) {
			}
			req.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
			req.send();
		});
		console.log("sessionKey:" + sessionKey);
		await new Promise(function (resolve, reject) {
			req = new XMLHttpRequest();
			req.open("PUT", "upload/" + sessionKey + "/set-metadata");
			req.onload = function (e) {
				if (req.readyState === req.DONE) {
					console.log("set-metadata result: " + req.responseText);
					if (req.status === 200) {
						resolve();
					} else {
						reject(req.responseText);
					}
				}
			}
			req.onabort = function (e) {
				reject("aborted");
			}
			req.onerror = function (e) {
			}
			req.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
			req.send("ownerKey=" + encodeURIComponent(elemInputOwnerKey.value));
		});

		var totalSize = 0;
		for (var fileName in filesToUpload) {
			totalSize += filesToUpload[fileName].size;
		}
		console.log("totalSize: " + (totalSize / 1024 / 1024) + "MB");

		elemProgressUpload.value = 0;
		var sendSize = 0;
		for (var fileName in filesToUpload) {
			var fileBlob = filesToUpload[fileName];
			console.log("file size: " + fileBlob.size );
			console.log("file type: " + fileBlob.type );
			var fileId = await new Promise(function (resolve, reject) {
				req = new XMLHttpRequest();
				req.open("PUT", "upload/" + sessionKey + "/begin-file");
				req.onload = function (e) {
					if (req.readyState === req.DONE) {
						console.log("begin-file result: " + req.responseText);
						if (req.status === 200) {
							resolve(req.responseText);
						} else {
							reject(req.responseText);
						}
					}
				}
				req.onabort = function (e) {
					reject("aborted");
				}
				req.onerror = function (e) {
				}
				req.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
				req.send("fileName=" + encodeURIComponent(fileName) + "&contentType=" + encodeURIComponent(fileBlob.type));
			});
			console.log("fileId: " + fileId);
			var sliceSize = 20 * 1024 * 1024;
			var offset = 0;
			while (offset < fileBlob.size) {
				var sliceBlob = fileBlob.slice(offset,offset+sliceSize);
				console.log("offset: " + offset );
				console.log("sliceSize: " + sliceSize );
				console.log("sliceBlob size: " + sliceBlob.size );
				await new Promise(function (resolve, reject) {
					var formData = new FormData();
					formData.append("file",sliceBlob);
					formData.append("fileId",fileId);
					req = new XMLHttpRequest();
					req.open("PUT", "upload/" + sessionKey + "/send-file");
					req.onload = function (e) {
						if (req.readyState === req.DONE) {
							console.log("send-file result: " + req.responseText);
							if (req.status === 200) {
								resolve();
							} else {
								reject(req.responseText);
							}
						}
					}
					req.onabort = function (e) {
						reject("aborted");
					}
					req.onerror = function (e) {
					}
					req.send(formData);
				});
				sendSize += sliceBlob.size;
				elemProgressUpload.value = sendSize / totalSize * 100;
				offset += sliceSize;
			}
			await new Promise(function (resolve, reject) {
				req = new XMLHttpRequest();
				req.open("PUT", "upload/" + sessionKey + "/end-file");
				req.onload = function (e) {
					if (req.readyState === req.DONE) {
						console.log("end-file result: " + req.responseText);
						if (req.status === 200) {
							resolve();
						} else {
							reject(req.responseText);
						}
					}
				}
				req.onabort = function (e) {
					reject("aborted");
				}
				req.onerror = function (e) {
				}
				req.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
				req.send("fileId=" + encodeURIComponent(fileId));
			});
		}
		await new Promise(function (resolve, reject) {
			req = new XMLHttpRequest();
			req.open("PUT", "upload/" + sessionKey + "/end-session");
			req.onload = function (e) {
				if (req.readyState === req.DONE) {
					console.log("end-session result: " + req.responseText);
					if (req.status === 200) {
						resolve();
					} else {
						reject(req.responseText)
					}
				}
			}
			req.onabort = function (e) {
				reject("aborted");
			}
			req.onerror = function (e) {
			}
			req.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
			req.send();
		});
		return await new Promise(function (resolve, reject) {
			resolve(sessionKey);
		});
	}

	elemButtonUpload.onclick = async function(e) {
		setButtons(false,true); // upload,cancel
		doPosts().then(function(sessionKey) {
			console.log("succress");
//			alert("upload complete: " + sessionKey);
			setButtons(false,false); // upload,cancel
			location.href = "share_" + sessionKey + ".html"
		},function(e) {
			alert(e);
			console.log(e);
			setButtons(true,false); // upload,cancel
		});
	}

	elemButtonCancel.onclick = function(e) {
		// [TODO] check req.readyState
		req.abort();
	}

    
};
