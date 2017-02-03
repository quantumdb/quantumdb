$(document).ready(() => {
	"use strict";

	const spy = () => {
		var winTop = $(window).scrollTop();
		var $divs = $('section');

		var top = $.grep($divs, function (item) {
			return $(item).position().top <= winTop + 128;
		});

		$('.docs-sidebar').find('a').parent().removeClass('in-viewport');

		if (top.length > 0) {
			var current = top[top.length - 1];
			var id = $(current).attr('id');
			if (id) {
				$('.docs-sidebar').find('a[href="#' + id + '"]').parent().addClass('in-viewport');
				return;
			}
		}

		$('.docs-sidebar').find('li').first().addClass('in-viewport');

	};

	$.attrHooks['viewbox'] = {
		set: function(elem, value, name) {
			elem.setAttributeNS(null, 'viewBox', value + '');
			return value;
		},
		get: function(elem, value, name) {
			return elem.getAttribute("viewBox");
		}
	};

	const resize = (svg) => {
		var newWidth = svg.width();
		const minWidth = svg.attr('min-width');
		if (minWidth && newWidth < minWidth) {
			newWidth = minWidth;
		}

		var originalViewBox = svg.attr('original-viewbox');
		if (!originalViewBox) {
			originalViewBox = svg.attr('viewBox');
			svg.attr('original-viewbox', originalViewBox);
		}

		const chunks = originalViewBox.split(' ');
		const left = parseInt(chunks[0]);
		const top = parseInt(chunks[1]);
		// const width = parseInt(chunks[2]);
		const height = parseInt(chunks[3]);

		svg.attr('viewBox', left + ' ' + top + ' ' + newWidth + ' ' + height);
	};

	var svgs = $('svg[resizable="true"]');
	const resizeAll = () => {
		svgs.each((index, element) => resize($(element)));
	};

	$(window).scroll(() => spy());
	$(window).resize(() => {
		resizeAll();
		spy();
	});

	resizeAll();
	spy();

});