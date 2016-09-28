function rt() {
	if (document.f.ql.value == "" && document.f.qf.value == ""
			&& document.f.qy.value == "" && document.f.qn.value == "") {
		window.location = "search.html";

		return false;
	}
}

function doSubmit() {
	if (document.f.qand.value == "" && document.f.qnot.value == ""
			&& document.f.qor.value == "" && document.f.qphrase.value == "") {
		location.href = "search.html";
	} else {

		document.search.qand.value = document.f.qand.value;
		document.search.qphrase.value = document.f.qphrase.value;
		document.search.qor.value = document.f.qor.value;
		document.search.qnot.value = document.f.qnot.value;
		document.search.where.value = document.f.where.value;
		document.search.format.value = document.f.format.value;
		document.search.when.value = document.f.when.value;
		document.search.site.value = document.f.site.value;
		document.search.hitsPerPage.value = document.f.hitsPerPage.value;
		document.search.submit();
	}
}

function BindEnter(obj){
	//使用document.getElementById获取到按钮对象
	var button = document.getElementById('AdvancedSearch');
	if(obj.keyCode == 13){
		button.click();
		obj.returnValue = false;
	}
}