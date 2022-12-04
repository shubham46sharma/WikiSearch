console.log("hello");

let btn = document.querySelector("#lucene");
let form = document.getElementById("myform");
console.log(btn);
//let displayList = document.getElementById("list");
let exerciseList = document.querySelector(".exercise-list");
//let link = document.getElementById("link");
//let index = document.getElementById("index");
console.log("event added");
function handleSubmit(e){
  e.preventDefault();
    let query = document.getElementById('query').value;
    console.log(query);
    var api = "http://localhost:8080/api/getSearchResults/LuceneSearch/";
    const myHeaders = new Headers();
    var url = api+query;

    let data = [];
const myRequest = new Request(url, {
  method: 'GET',
  headers: myHeaders,
  mode: 'cors',
  cache: 'default',
});
    fetch(myRequest)
      .then(res=>res.json()).then((res)=>{
      data = res;
      console.log(data);
      if(data.length){
        data.forEach(element => {
          // const index = document.createElement("div");
          // index.append(element.index);
          const heading = document.createElement("h4");
          heading.append(element.index+ ". "+element.heading);
          const url = document.createElement("div");
          url.innerHTML = '<a href="'+element.url+'" rel="external" data-direction="reverse">'+element.url+'</a>';
          exerciseList.append(index, heading, url);
          // index.append(`${element.index}`);
          // displayList.append(`${element.heading}`);
          // link.append(`${element.url}`);

        });
      }
      }).catch(err=>console.error(err));

    }

    function handleLucene(){
      let use = document.getElementById("using");
      const lucene = document.createElement("h4");
      lucene.append("Indexed Using Lucene!");
      use.append(lucene);
    }
    function handleHadoop(){
      let use = document.getElementById("using");
      const lucene = document.createElement("h4");
      lucene.append("Indexed Using Hadoop!");
      use.append(lucene);
    }
form.addEventListener('submit',handleSubmit);
btn.addEventListener('click', handleLucene);
let hdp = document.getElementById("hadoop");
hdp.addEventListener('click', handleHadoop);
