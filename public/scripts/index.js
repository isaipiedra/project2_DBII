const tab_navigation = document.querySelector('.tab_navigation');
const nav_items = Array.from(tab_navigation.getElementsByTagName('li'));

nav_items.forEach(item => {
    item.addEventListener('click', () => {
            nav_items.forEach(nav_item => {
            nav_item.removeAttribute('id');
        });

        item.id = "selected";
    });
});
