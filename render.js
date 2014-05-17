var page = require('webpage').create(),
    system = require('system'),
    address, output, size;

if (system.args.length < 3) {
  console.log('Usage: render.js <url> <destination>');
  phantom.exit(1);
}

address = system.args[1];
destination = system.args[2];

console.log('Rendering ' + address + ' to ' + destination);

page.open(address, function() {
    page.render(destination);
    phantom.exit();
});
