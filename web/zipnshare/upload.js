window.onload = function() {

    var elemForm1 = document.getElementById("form1");
    var elemDivFileDrop = document.getElementById("divFileDrop");
    var elemInputFileSelect = document.getElementById("inputFileSelect");
    var elemDivFilesToUpload = document.getElementById("divFilesToUpload");
    var elemInputPassword = document.getElementById("inputPassword");
    var elemInputProgressUpload = document.getElementById("inputProgressUpload");
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
    elemButtonUpload.onclick = function(e) {
	var formData = new FormData();
	for(var fileName in filesToUpload) {
	    formData.append("file",filesToUpload[fileName]);
	}
	formData.append("password",elemInputPassword.value);

	req = new XMLHttpRequest();
	req.onload = function(e) {
	    if (req.readyState === req.DONE) {
		if (req.status === 200) {
		    setButtons(true,true); // upload,cancel
		    stopProgress(100);
		    location.href = "download_" + req.responseText + ".html"
		} else {
		    setButtons(true,false); // upload,cancel
		    stopProgress(0);
		}
	    }
	}
	req.onerror = function(e) {
	    alert("upload error");
	    setButtons(true,false); // upload,cancel
	    stopProgress(0);
	}
	req.onabort = function(e) {
	    alert("upload canceled");
	    setButtons(true,false); // upload,cancel
	    stopProgress(0);
	}
	req.onprogress = function(e) {
	    // [MEMO] unusable ? e.total is uncomputable
	    return ;
	    console.log("req.onprogress");
	    if (e.lengthComputable) {
		var uploadp = e.loaded / e.total * 100;
		console.log("uploadp: " + uploadp);
		elemInputProgressUpload.value = uploadp;
	    } else {
		console.log("uploadp: uncomputable");
	    }
	}
	req.open("POST", 'upload');
	req.send(formData);

	setButtons(false,true); // upload,cancel
	startProgress();
    }

    elemButtonCancel.onclick = function(e) {
	req.abort();
    }


    var progressPercent = 0;
    var progressInterval;
    function startProgress(){
	progressInterval = setInterval(function(){
	    elemInputProgressUpload.value = progressPercent;
	    progressPercent += 10;
	    if (progressPercent > 95) {
		progressPercent = 0;
	    }
	}, 200);
    }
    function stopProgress(val){
	clearInterval(progressInterval);
	elemInputProgressUpload.value = val;
    }
    
};
