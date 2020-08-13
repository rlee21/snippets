const allPaContainer = document.getElementById('see-all-pa-link-container')
const seeAllPaLink = document.getElementById('see-all-pa-link')

function updatePaLinkContainerBackgroundColor(color) {
  allPaContainer.style.backgroundColor = color;
}

function callback(mutationsList, observer) {
  let currentPaLinkContainerBackgroundColor = window.getComputedStyle(allPaContainer);
  let updatedColor = currentPaLinkContainerBackgroundColor.getPropertyValue('background-color') === 'rgb(255, 255, 255)' ? 'rgb(249, 249, 249)' : 'rgb(255, 255, 255)';
  mutationsList.forEach(function(mutation) {
    if (mutation.attributeName === 'class') {
      updatePaLinkContainerBackgroundColor(updatedColor);
    }
  })
}

const mutationObserver = new MutationObserver(callback);
mutationObserver.observe(seeAllPaLink, { attributes: true });

const hideAllPaLinkBottom = document.getElementById('hide-all-pa-link-bottom');

window.addEventListener('scroll', function() {
  if (window.scrollY > (allPaContainer.offsetTop + allPaContainer.offsetHeight)) {
      hideAllPaLinkBottom.style.display = 'none';
  } else {
      hideAllPaLinkBottom.style.display = 'inline';
   }
});

