// Copyright (C) 2011 by Ross Korsky
//
// This software is released under the MIT License that follows
//
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE



module("algorithm prototype", {
	grades: [
    {letter:"A", value:90},
    {letter:"B", value:80},
    {letter:"C", value:70},
    {letter:"D", value:60},
    {letter:"E", value:50},
    {letter:"F", value:40}
  ],
  students: [
    {id:1,name:"Fred"},
    {id:2,name:"Sam"},
    {id:3,name:"Bob"},
  ],
  tests: [
    {id:1,name:"Chapters 1-8"},
    {id:2,name:"Chapters 9-16"},
    {id:3,name:"Chapters 17-23"},
  ],
  scores: [
    {student_id:1, test_id:1, grade:80},
    {student_id:2, test_id:2, grade:60},
    {student_id:1, test_id:2, grade:90},
    {student_id:2, test_id:1, grade:70},
    {student_id:1, test_id:3, grade:90},
    {student_id:4, test_id:3, grade:40}
  ],
  verify_all_score_innerjoin: function( records ) {
    equals( records.length, 5, "record count" );
    equals( records[0].scores, this.scores[0] );
    equals( records[0].grades, this.grades[1] );
    equals( records[0].students, this.students[0] );
    equals( records[0].tests, this.tests[0] );
    
    equals( records[1].scores, this.scores[1] );
    equals( records[1].grades, this.grades[3] );
    equals( records[1].students, this.students[1] );
    equals( records[1].tests, this.tests[1] );
   
    equals( records[2].scores, this.scores[2] );
    equals( records[2].grades, this.grades[0] );
    equals( records[2].students, this.students[0] );
    equals( records[2].tests, this.tests[1] );
   
    equals( records[3].scores, this.scores[3] );
    equals( records[3].grades, this.grades[2] );
    equals( records[3].students, this.students[1] );
    equals( records[3].tests, this.tests[0] );
   
    equals( records[4].scores, this.scores[4] );
    equals( records[4].grades, this.grades[0] );
    equals( records[4].students, this.students[0] );
    equals( records[4].tests, this.tests[2] );
  },
  verify_all_score_leftjoin: function( records ) {
    equals( records.length, 6, "record count" );
    equals( records[0].scores, this.scores[0] );
    equals( records[0].grades, this.grades[1] );
    equals( records[0].students, this.students[0] );
    equals( records[0].tests, this.tests[0] );
    
    equals( records[1].scores, this.scores[1] );
    equals( records[1].grades, this.grades[3] );
    equals( records[1].students, this.students[1] );
    equals( records[1].tests, this.tests[1] );
   
    equals( records[2].scores, this.scores[2] );
    equals( records[2].grades, this.grades[0] );
    equals( records[2].students, this.students[0] );
    equals( records[2].tests, this.tests[1] );
   
    equals( records[3].scores, this.scores[3] );
    equals( records[3].grades, this.grades[2] );
    equals( records[3].students, this.students[1] );
    equals( records[3].tests, this.tests[0] );
   
    equals( records[4].scores, this.scores[4] );
    equals( records[4].grades, this.grades[0] );
    equals( records[4].students, this.students[0] );
    equals( records[4].tests, this.tests[2] );
   
    equals( records[5].scores, this.scores[5] );
    equals( records[5].grades, this.grades[5] );
    same( records[5].students, {} );
    equals( records[5].tests, this.tests[2] );
  }
});

test("left join", function() {
  var pivot = DataManip.pivot;
  
  var records = [];
  var selector = function( ) {
      return DataManip.extend( {}, this );
  };

  var idx_grades_id = pivot( this.grades, "value" );
  var idx_students_id = pivot( this.students, "id" );
  var idx_tests_id = pivot( this.tests, "id" );

  var context = {};
  for( var scores_iter in this.scores ) {
    context["scores"] = this.scores[scores_iter];
    
    var vals_grades = idx_grades_id[ context["scores"]["grade"] ] || [{}]; //left
    for( var grades_iter in vals_grades ) {
      context["grades"] = vals_grades[grades_iter];
      
      var vals_students = idx_students_id[ context["scores"]["student_id"] ] || [{}]; //left
      for( var students_iter in vals_students ) {
        context["students"] = vals_students[students_iter];
        
        var vals_tests = idx_tests_id[ context["scores"]["test_id"] ] || [{}]; //left
        for( var tests_iter in vals_tests ) {
          context["tests"] = vals_tests[students_iter];
          
          var selected = selector.call( context );
          if ( selected == context ) selected = DataManip.extend( {}, this );
          if ( selected ) records.push( selected );
          
          delete( context["tests"] );
        }
        delete( context["students"] );
      }
      delete( context["grades"] );
    }
    delete( context["scores"] );
  }
  this.verify_all_score_leftjoin( records );
});


test("inner join", function() {
  var pivot = DataManip.pivot;
  
  var records = [];
  var selector = function( ) {
      return DataManip.extend( {}, this );
  };

  var idx_grades_id = pivot( this.grades, "value" );
  var idx_students_id = pivot( this.students, "id" );
  var idx_tests_id = pivot( this.tests, "id" );

  var context = {};
  var vals_scores = this.scores;
  for( var scores_iter in vals_scores ) {
    context["scores"] = vals_scores[scores_iter];
    
    var vals_grades = idx_grades_id[ context["scores"]["grade"] ]; //inner
    for( var grades_iter in vals_grades ) {
      context["grades"] = vals_grades[grades_iter];
      
      var vals_students = idx_students_id[ context["scores"]["student_id"] ];//inner
      for( var students_iter in vals_students ) {
        context["students"] = vals_students[students_iter];
        
        var vals_tests = idx_tests_id[ context["scores"]["test_id"] ];//inner
        for( var tests_iter in vals_tests ) {
          context["tests"] = vals_tests[students_iter];
          
          var selected = selector.call( context );
          if ( selected == context ) selected = DataManip.extend( {}, this );
          if ( selected ) records.push( selected );
          
          delete( context["tests"] );
        }
        delete( context["students"] );
      }
      delete( context["grades"] );
    }
    delete( context["scores"] );
  }
  this.verify_all_score_innerjoin( records );
});


module("DataManip::DataJoin - using directives", {
	grades: [
    {letter:"A", value:90},
    {letter:"B", value:80},
    {letter:"C", value:70},
    {letter:"D", value:60},
    {letter:"E", value:50},
    {letter:"F", value:40}
  ],
  students: [
    {id:1,name:"Fred"},
    {id:2,name:"Sam"},
    {id:3,name:"Bob"},
  ],
  tests: [
    {id:1,name:"Chapters 1-8"},
    {id:2,name:"Chapters 9-16"},
    {id:3,name:"Chapters 17-23"},
  ],
  scores: [
    {student_id:1, test_id:1, grade:80},
    {student_id:2, test_id:2, grade:60},
    {student_id:1, test_id:2, grade:90},
    {student_id:2, test_id:1, grade:70},
    {student_id:1, test_id:3, grade:90},
    {student_id:4, test_id:3, grade:40}
  ],
  verify_all_score_innerjoin: function( records ) {
    equals( records.length, 5, "record count" );
    equals( records[0].scores, this.scores[0] );
    equals( records[0].grades, this.grades[1] );
    equals( records[0].students, this.students[0] );
    equals( records[0].tests, this.tests[0] );
    
    equals( records[1].scores, this.scores[1] );
    equals( records[1].grades, this.grades[3] );
    equals( records[1].students, this.students[1] );
    equals( records[1].tests, this.tests[1] );
   
    equals( records[2].scores, this.scores[2] );
    equals( records[2].grades, this.grades[0] );
    equals( records[2].students, this.students[0] );
    equals( records[2].tests, this.tests[1] );
   
    equals( records[3].scores, this.scores[3] );
    equals( records[3].grades, this.grades[2] );
    equals( records[3].students, this.students[1] );
    equals( records[3].tests, this.tests[0] );
   
    equals( records[4].scores, this.scores[4] );
    equals( records[4].grades, this.grades[0] );
    equals( records[4].students, this.students[0] );
    equals( records[4].tests, this.tests[2] );
  },
  verify_all_score_leftjoin: function( records ) {
    equals( records.length, 6, "record count" );
    equals( records[0].scores, this.scores[0] );
    equals( records[0].grades, this.grades[1] );
    equals( records[0].students, this.students[0] );
    equals( records[0].tests, this.tests[0] );
    
    equals( records[1].scores, this.scores[1] );
    equals( records[1].grades, this.grades[3] );
    equals( records[1].students, this.students[1] );
    equals( records[1].tests, this.tests[1] );
   
    equals( records[2].scores, this.scores[2] );
    equals( records[2].grades, this.grades[0] );
    equals( records[2].students, this.students[0] );
    equals( records[2].tests, this.tests[1] );
   
    equals( records[3].scores, this.scores[3] );
    equals( records[3].grades, this.grades[2] );
    equals( records[3].students, this.students[1] );
    equals( records[3].tests, this.tests[0] );
   
    equals( records[4].scores, this.scores[4] );
    equals( records[4].grades, this.grades[0] );
    equals( records[4].students, this.students[0] );
    equals( records[4].tests, this.tests[2] );
   
    equals( records[5].scores, this.scores[5] );
    equals( records[5].grades, this.grades[5] );
    same( records[5].students, {} );
    equals( records[5].tests, this.tests[2] );
  }
});

test("left join", function() {
  var jb = new DataJoin();
  jb.directives = [
    { join: 'from', arg: 0, as: 'scores' },
    { join: 'left', arg: 1, as: 'grades'  , on: { 'grades': 'value', 'scores': 'grade' } },
    { join: 'left', arg: 2, as: 'students', on: { 'students': 'id' , 'scores': 'student_id' } },
    { join: 'left', arg: 3, as: 'tests'   , on: { 'tests': 'id'    , 'scores': 'test_id' } } 
  ];
  var foo = jb.compile();
  
  var records = foo( this.scores, this.grades, this.students, this.tests );
  this.verify_all_score_leftjoin( records );
});

test("inner join", function() {
  var jb = new DataJoin();
  jb.directives = [
    { join: 'from' , arg: 0, as: 'scores' },
    { join: 'inner', arg: 1, as: 'grades'  , on: { 'grades': 'value', 'scores': 'grade' } },
    { join: 'inner', arg: 2, as: 'students', on: { 'students': 'id' , 'scores': 'student_id' } },
    { join: 'inner', arg: 3, as: 'tests'   , on: { 'tests': 'id'    , 'scores': 'test_id' } } 
  ];
  var foo = jb.compile();
  
  var records = foo( this.scores, this.grades, this.students, this.tests );
  this.verify_all_score_innerjoin( records );
});

test("cross join", function() {
  var jb = new DataJoin();
  jb.directives = [
    { join: 'from' , arg: 0, as: 'students' },
    { join: 'cross', arg: 1, as: 'grades' },
    { join: 'cross', arg: 2, as: 'tests' } 
  ];
  var foo = jb.compile();
  
  var records = foo( this.students, this.grades, this.tests );
  equals( records.length, this.students.length * this.grades.length * this.tests.length );
});



module("DataManip::DataJoin - transitive joining", {
  students: [
    {id:1,name:"Fred"},
    {id:2,name:"Sam"},
    {id:3,name:"Bob"},
  ],
  contact_info: [
    {student_id:1, phone:'555-1212', sibling_id: 2},
    {student_id:1, phone:'555-9876', sibling_id: 2},
    {student_id:3, phone:'555-1234'}
  ],
  scores: [
    {student_id:1, test_id:1, grade:80},
    {student_id:2, test_id:2, grade:60},
    {student_id:1, test_id:2, grade:68},
    {student_id:2, test_id:1, grade:80},
    {student_id:1, test_id:3, grade:50},
    {student_id:4, test_id:3, grade:40}
  ]
});

/**
 * this test demonstrates many powerful features of the DataJoin including
 *   - filtering
 *   - using the same 'table' multiple times (arg1)
 *   - returning a string from the selector
 *   - and transitive joining of 'tables'
 */
test("left join", function() {
  var jb = new DataJoin();
  jb.directives = [
    { join: 'from', arg: 0, as: 'scores' },
    { join: 'inner', arg: 1, as: 'student', on: { 'student': 'id' , 'scores': 'student_id' } },
    { join: 'inner', arg: 2, as: 'contact' , on: { 'contact': 'student_id', 'student': 'id' } },
    { join: 'inner', arg: 1, as: 'sibling' , on: { 'sibling': 'id', 'contact': 'sibling_id' } } 
  ];
  jb.selector = function() {
    if ( this.scores.grade >= 70 ) return null;
    return "test grade " + this.scores.grade 
      + " student " + this.student.name 
      + " phone " + this.contact.phone
      + " sibling " + this.sibling.name;
  }
  var foo = jb.compile();
  
  var records = foo( this.scores, this.students, this.contact_info );
  equals( records.length, 4 );
  equals( records[0], "test grade 68 student Fred phone 555-1212 sibling Sam" );
  equals( records[1], "test grade 68 student Fred phone 555-9876 sibling Sam" );
  equals( records[2], "test grade 50 student Fred phone 555-1212 sibling Sam" );
  equals( records[3], "test grade 50 student Fred phone 555-9876 sibling Sam" );
});

test("change selector after compilation", function() {
  var jb = new DataJoin();
  jb.directives = [
    { join: 'from', arg: 0, as: 'scores' },
    { join: 'inner', arg: 1, as: 'student', on: { 'student': 'id' , 'scores': 'student_id' } },
    { join: 'inner', arg: 2, as: 'contact' , on: { 'contact': 'student_id', 'student': 'id' } },
    { join: 'inner', arg: 1, as: 'sibling' , on: { 'sibling': 'id', 'contact': 'sibling_id' } } 
  ];
  var foo = jb.compile();
  
  // the selector can be changed after compilation
  foo.external.selector = function() {
    if ( this.scores.grade >= 70 ) return null;
    return "test grade " + this.scores.grade 
      + " student " + this.student.name 
      + " phone " + this.contact.phone
      + " sibling " + this.sibling.name;
  }
  
  var records = foo( this.scores, this.students, this.contact_info );
  equals( records.length, 4 );
  equals( records[0], "test grade 68 student Fred phone 555-1212 sibling Sam" );
  equals( records[1], "test grade 68 student Fred phone 555-9876 sibling Sam" );
  equals( records[2], "test grade 50 student Fred phone 555-1212 sibling Sam" );
  equals( records[3], "test grade 50 student Fred phone 555-9876 sibling Sam" );
});

test("treating missing/null arguments as empty", function() {
  var jb = new DataJoin();
  jb.directives = [
    { join: 'from' , arg: 0, as: 'scores' },
    { join: 'left', arg: 1, as: 'grades'  , on: { 'grades': 'value', 'scores': 'grade' } },
    { join: 'left', arg: 2, as: 'students', on: { 'students': 'id' , 'scores': 'student_id' } },
    { join: 'left', arg: 3, as: 'tests'   , on: { 'tests': 'id'    , 'scores': 'test_id' } }, //leave last comma for IE check
  ];
  var foo = jb.compile();
  
  var records = foo( this.scores, this.grades, null, this.tests );
  equals( records.length, this.scores.length );
  equals( records[0].scores, this.scores[0] );
  same( records[0].grades, {} );
  same( records[0].students, {} );
  same( records[0].tests, {} );
});


module("DataManip::DataJoin - parser");

test("sanity", function(){
  var query = "FROM arg0 AS foo JOIN arg1 as bar1 ON foo.bar1_id = bar1.id "
            + "inner join ARG1 AS bar2 on foo.bar2_id=bar2.id "
            + "left\tjoin\tARG1\tAS\tbar3\ton\tbar1.bar3_id\t=\tbar3.bar_id";

  var joiner = new DataJoin();
  equals( joiner.parse( query ), joiner, "parse returns self for chaining" );
  
  equals( joiner.directives.length, 4 );
  
  same( joiner.directives[0], { join: 'from', arg: 0, as: 'foo'} );
  same( joiner.directives[1], { join: 'inner', arg: 1, as: 'bar1', on: { foo: 'bar1_id', bar1: 'id' } } );
  same( joiner.directives[2], { join: 'inner', arg: 1, as: 'bar2', on: { foo: 'bar2_id', bar2: 'id' } } );
  same( joiner.directives[3], { join: 'left', arg: 1, as: 'bar3', on: { bar1: 'bar3_id', bar3: 'bar_id' } } );
});

































