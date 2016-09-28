$(function() {
	var $search = $('#search');
	var $searchInput = $search.find('#search-text');

	var $autocomplete = $('<div class="autocomplete"></div>').hide()
			.insertAfter('#submit');

var clear = function() {
		$autocomplete.empty().hide();
	};

	$searchInput.blur(function() {
		setTimeout(clear, 500);
	});
	var selectedItem = null;
	var timeoutid = null;
	var setSelectedItem = function(item) {
		selectedItem = item;
		if (selectedItem < 0) {
			selectedItem = $autocomplete.find('li').length - 1;
		} else if (selectedItem > $autocomplete.find('li').length - 1) {
			selectedItem = 0;
		}
		$autocomplete.find('li').removeClass('autohighlight').eq(selectedItem)
				.addClass('autohighlight');
	};
	var ajax_request = function() {
		// ajax服务端通信
		$.ajax({
			'url' : '../suggest.jsp',
			'data' : {
				'search-text' : escape($searchInput.val())
			},
			'dataType' : 'json',
			'type' : 'POST',
			'success' : function(data) {
				if (data.length) {
					$.each(data, function(index, term) {
						$('<li></li>').text(term).appendTo($autocomplete)
								.addClass('clickable').hover(
										function() {
											$(this).siblings().removeClass(
													'autohighlight');
											$(this).addClass('autohighlight');
											selectedItem = index;
										}, function() {
											$(this).removeClass('autohighlight');
											selectedItem = -1;
										}).click(function() {
									$searchInput.val(term);
									$autocomplete.empty().hide();
								});
					});
					// 设置下拉列表的位置，然后显示下拉列表
					var ypos = $searchInput.top;
					var xpos = $searchInput.left;
					var size = $searchInput.attr('size');
					$autocomplete.css('width', $searchInput.css('width'));
					$autocomplete.css('background-color', '#FFFFFF');
					if(size == 40){
						$autocomplete.css({
							'position' : 'relative',
							'left' : -40 + "px",
							'top' : ypos + "px"
						});
					}else{
						$autocomplete.css({
							'position' : 'absolute',
							'left' : xpos + "px",
							'top' : ypos + "px"
						});
					}
					setSelectedItem(0);
					$autocomplete.show();
				}
			}
		});
	};
	
	// 对输入框进行事件注册
	$searchInput.keyup(function(event) {
		if (event.keyCode > 40 || event.keyCode == 8 || event.keyCode == 32) {
			$autocomplete.empty().hide();
			clearTimeout(timeoutid);
			timeoutid = setTimeout(ajax_request, 100);
		} else if (event.keyCode == 38) {
			if (selectedItem == -1) {
				setSelectedItem($autocomplete.find('li').length - 1);
			} else {
				setSelectedItem(selectedItem - 1);
			}
			event.preventDefault();
		} else if (event.keyCode == 40) {
			if (selectedItem == -1) {
				setSelectedItem(0);
			} else {
				setSelectedItem(selectedItem + 1);
			}
			event.preventDefault();
		}
	}).keypress(function(event) {
		if (event.keyCode == 13) {
			if ($autocomplete.find('li').length == 0 || selectedItem == -1) {
				return;
			}
			$searchInput.val($autocomplete.find('li').eq(selectedItem).text());
			$autocomplete.empty().hide();
			event.preventDefault();
		}
	}).keydown(function(event) {
		if (event.keyCode == 27) {
			$autocomplete.empty().hide();
			event.preventDefault();
		}
	});
	
	// 注册窗口大小改变的事件，重新调整下拉列表的位置
	$(window).resize(function() {
		var ypos = $searchInput.top;
		var xpos = $searchInput.left;
		var size = $searchInput.attr('size');
		$autocomplete.css('width', $searchInput.css('width'));
		$autocomplete.css('background-color', '#FFFFFF');
		if(size == 40){
			$autocomplete.css({
				'position' : 'relative',
				'left' : -40 + "px",
				'top' : ypos + "px"
			});
		}else{
			$autocomplete.css({
				'position' : 'absolute',
				'left' : xpos + "px",
				'top' : ypos + "px"
			});
		}
	});
});