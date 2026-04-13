const regex = /\b([A-Z]{3})\s+(?:[a-zA-ZÄÖÜäöüßé]{1,4}\s+)?([A-ZÄÖÜa-zßäöüé][\wßäöüÄÖÜé\-\s/\.]+)/;
const text1 = "GER NS Munster";
const text2 = "GER HH Hamburg";
const text3 = "[Provided by...] \n GER HH Hamburg";
console.log(text1.match(regex));
console.log(text2.match(regex));
console.log(text3.match(regex));
