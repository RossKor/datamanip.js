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

var DataManip = {};
(function(){

/**
 * pivots records on the given field (member). structure of the result
 * is controlled by value of the 'flat' argument.
 *
 * @param records records to act on
 * @param field member name to pivot on
 * @param flat boolean, controls result structure
 *             true - map of values is returned (value selected is the 'last' matching), 
 *             false - (default) map of arrays of values is returned.
 * @returns pivoted view of records
 */
function pivot( records, field, flat ) {
  var out = {};
  flat = typeof(flat) == 'undefined' ? false : flat;
  
  if ( !flat ) {
    for( var i = 0, len = records.length; i < len; i ++ ) {
      var r = records[i];
      // skip bad entries - these occur in IE under the additional case 
      // [{},{},] note the trailing ,
      if ( !r ) continue; 
      var a = out[r[field]];
      if ( !a ) {
        a = [];
        out[r[field]] = a;
      }
      a.push( r );
    }
  } else {
    for( var i = 0, len = records.length; i < len; i ++ ) {
      var r = records[i];
      if ( !r ) continue; 
      var a = out[r[field]] = r;
    }
  }
  
  return out;
}

/**
 * filters records based on the given field name and value
 * a record must both have a field with the given name and that fields
 * value must match the given value to be selected.
 *
 * @param records records to act on
 * @param name seeking field name
 * @param value seeking field value
 */
function filter( records, name, value ) {
  var items = [];
  for( var i = 0, len = records.length; i < len; i ++ ) {
    var r = records[i];
    if ( !r ) continue; 
    var a = r[name];
    if ( a && a == value ) items.push( r );
  }
  return items;
}

/**
 * Copy all of the properties in the source objects over to the destination
 * object. It's in-order, to the last source will override properties of the
 * same name in previous arguments.
 * 
 * Modified and taken from underscore.js
 *
 * @param obj object to extend
 * @param args objects to extend with
 * @return obj given for chaining
 */
var extend = function( obj ) {
  for( var i = 1, len = arguments.length; i < len; i ++ ) {
    var source = arguments[1];
    for (var prop in source) obj[prop] = source[prop];
  }
  return obj;
};

/**
 * @class DataJoin
 * Used to construct and compile SQL like 'FROM .. JOIN ..' statements into a 
 * javascript function which can then be used for multiple evaluations of
 * similar data sets.
 *
 * Supported joins:
 *   FROM       - (required) used as the base iterator target, every record
 *                is visited. If occurring more than once treated as CROSS.
 *   LEFT JOIN  - Return all rows from the left table, even if there are no 
 *                matches in the right table.
 *   INNER JOIN / JOIN - Return rows when there is at least one match in both
 *                       tables  
 *   CROSS JOIN - Cartesian product of the sets of rows from the joined tables.
 *                Each row from the first table is combined with each row from
 *                the second table and so on.
 *
 *
 * Directives:
 *   Directives are used to generate the javascript which will perform the join.
 *
 *   The directives content is considered 'trusted' and therefore directives are
 *   a possible JS injection vector (sanitize user input before directly
 *   creating directives).
 *
 *   The parse method may be used to create directives for you by using an SQL
 *   fragment like syntax.
 *   
 *   Directives have the following structure
 *     - join: One of 'from', 'left', 'inner', 'cross'
 *     - arg : Numeric, index of the function parameter which will be referenced
 *             by the alias established by 'as'. Does not need to be unique.
 *     - as  : Alias which will be used to refer to 'this' view of the specified
 *             function argument.
 *     - on  : Structured containing exactly two &lt;alias&gt;: &lt;field&gt;
 *             entries, one of which MUST be the same value as 'as'.
 *
 *  Notice:
 *    - arg#'s may be reused just as you can join a table on itself in SQL
 *    - aliases must be unique within a set of directives
 *    - only equality '=' is supported at this time
 *    - having a FROM statement is required
 *    - joins must have an ON statement that contains reference to the name
 *      aliased by 'as'
 *    - The parse method will throw Error's for common failure cases but this
 *      error checking may not catch all invalid cases.
 *    - Currently directives are not reordered by this system and the order 
 *      of directives is used to build the join function code. This means that, 
 *      for now, join dependency MUST be satisfied by the user by the order 
 *      of listing. 
 * 
 * 
 * The user provided 'selector' function:
 *   The selector function like a combination of an SQL SELECT clause and WHERE 
 *   clause. It is used by the compiled join function to perform record selection
 *   as well as creating a record 'view' or transformation.
 *   
 *   The selector function is called in the context of the current, joined, row.
 *   In other words this function references 'tables' by their alias through the
 *   'this' object. For example <code>this.score</code> will refer to the record
 *   that was declared to have the alias of 'score' (via 'as') in the
 *   directives.
 *   
 *   Performing 'WHERE' like actions
 *     The selector function is capable of handling 'where' clauses. By 
 *     returning null or undefined the record will be omitted from the result.
 *   
 *   Take note that the return value from the selector is pushed into an array
 *   and that array is what is returned from the compiled join function upon
 *   its invocation. 
 *   This has several powerful implications such as...
 *     - Returning 'this' causes the result record to have the same structure as
 *       'this' ('this' is cloned and pushed to the result set).
 *     - Returning a contrived anonymous object results in a set of records
 *       having whatever structure that object defines (e.g.
 *       <code>return {name: this.user.name, group: this.group.name};</code>)
 *     - Returning a string or other simple value causes the result record set
 *       to simply be a list of these primitives.
 *     - Returning null or undefined results in no value being pushed to the
 *       result set for this data combination.
 *     - Complex actions such as DOM manipulation can occur within the select
 *       function in addition to, or instead of, actually building a list of
 *       records.
 *   
 *   The default selector simply returns the full set of records ('this')
 *
 *   The selector may be changed after the function is compiled by setting the
 *   bound selector at foo.external.selector where foo is a compiled join 
 *   function. Changing the selector on a DataJoin object will not affect any
 *   previously compiled join functions.
 *   
 *   
 * The parse() method:
 *   The parse() method allows you to specify directives in a, human friendly,
 *   SQL like syntax consisting of the FROM and JOIN portions of an SQL
 *   statement.
 *  
 *   Example statement which may be passed to parse():
 *     FROM arg0 AS score
 *     LEFT JOIN arg1 AS grade   ON grade.id   = score.grade_id
 *     LEFT JOIN arg2 AS student ON student.id = score.student_id
 *     LEFT JOIN arg3 AS test    ON test.id    = score.test_id
 *  
 *  Notice:
 *    - see Directives notices
 *  
 * Usage example:
 *   Craeate instance: 
 *     <code>var jb = new DataJoin();</code>
 *
 *   Set directives in one of the following ways:
 *     <code>jb.directives = [
 *       { join: 'from' , arg: 0, as: 'score' },
 *       { join: 'left' , arg: 1, as: 'grade'  , on: { 'grade': 'id'  , 'score': 'grade_id' } },
 *       { join: 'inner', arg: 2, as: 'student', on: { 'student': 'id', 'score': 'student_id' } },
 *       { join: 'left' , arg: 3, as: 'test'   , on: { 'test': 'id'   , 'score': 'test_id' } } 
 *     ];</code>
 *   -- OR --
 *     <code>jb.parse(
 *       "FROM arg0 AS score " +
 *       "LEFT JOIN arg1 AS grade   ON grade.id   = score.grade_id " +
 *       "LEFT JOIN arg2 AS student ON student.id = score.student_id " +
 *       "LEFT JOIN arg3 AS test    ON test.id    = score.test_id "
 *     );</code>
 *
 *   Set the selector function:
 *     this selector will result in a set of records containing only test_score less than 70 
 *     <code>jb.selector = function(){ 
 *       if ( this.score.test_score >= 70 ) return null;
 *       return { student_name: student.name, grade: this.grade.letter, test_name: this.test.name };
 *     }</code>
 *
 *   Call .compile and either retain the function returned or reference via .exec later
 *
 * Calling the generated function:
 *   given lists of simple objects of the same name as aliased in the directive
 *   example call
 *     <code>var records = jb.exec( scores, grades, students, tests )</code>
 *
 * NOTICE
 *   - Directives must be listed in dependency order (if c->b->a then directive
 *     order must be a, b, c). This is true when setting directives using 
 *     either method (calling .parse or setting .directive).
 *   - Directive arguments can be in any order and be non-contiguous
 *   - The generated function is completely independent from the builder (once 
 *     compiled changing the selector will not change the selector used by the
 *     compiled function but the new selector will be used by any function as
 *     the product of calling compile again).
 *   - The directive value 'join' can be one of: from, left, or inner. one, and
 *     only one, 'from' is a required directive
 *   - Behavior of having multiple from statements is equivalent to performing 
 *     CROSS JOIN. 
 *   - Mixing CROSS joins with other join types may result in unexpected, but 
 *     deterministic, behavior if/when directive ordering is implemented in the
 *     future.
 */
function DataJoin() {
  this.directives = [];
  this.selector = DataJoin__defaultSelector;
  return;
}

/**
 * set to '\n' to debug generated code
 * default is an empty string
 */
DataJoin.prototype.__eol__ = '';

/**
 * this selector is used by the generated function if no selector is specified
 */
var DataJoin__defaultSelector = function() { return this; }

/**
 * Generates the code to perform the join specified by the directives and
 * returns a compiled function. Also sets this same function as the member
 * this.exec
 *
 * The compiled function should be called passing a set of arrays matching 
 * the order specified by the 'arg: #' value of each directive.
 *
 * Changing the members of exec.external is allowed and makes it possible to
 * change the select function used by the compiled join function.
 *
 * For example:
 *    given directives = [
 *       { join: 'from', arg: 0, as: 'score' },
 *       { join: 'left', arg: 1, as: 'grade'  , on: { 'grade': 'id'  , 'score': 'grade_id' } },
 *       { join: 'left', arg: 2, as: 'student', on: { 'student': 'id', 'score': 'student_id' } },
 *       { join: 'left', arg: 3, as: 'test'   , on: { 'test': 'id'   , 'score': 'test_id' } } 
 *     ];
 *
 *   given lists of simple objects of the same name as aliased in the directive example
 *   var records = jb.exec( scores, grades, students, tests );
 */
DataJoin.prototype.compile = function() {
  this.__code = DataJoin__generate.call( this );
  // functions/objects  placed within 'external' are made available 
  // runtime (swappable) via exec.external.* this allows for the selector
  // function to be changed after this call to compile
  var context = {
    external: { 
      selector: this.selector
    },
    extend: extend,
    pivot: pivot
  }
  
  this.exec = (function( ctx, func ) {
    var foo = function() {
      return func.apply( ctx, arguments );
    }
    foo.external = ctx.external;
    return foo;
  })( context, new Function( this.__code ) );
  
  return this.exec;
}

/**
 * generates the javascript code to perform arbitrary join operations 
 */
var DataJoin__generate = function() {
  var argMap = {};
  var lookupMap = {};
  var code = '';
  var eol = this.__eol__;
  
  // emit function aliasing
  code += 'var selector=this.external.selector,' + eol;
  code +=     'extend=this.extend,' + eol;
  code +=     'pivot=this.pivot;' + eol;
  
  // emit other members
  code += 'var r=[],' + eol; //records
  code +=     'x={};' + eol; //context
  
  // build list of arguments and lookups needed
  for( var i = 0, len = this.directives.length; i < len; i ++ ) {
    var directive = this.directives[i];
    if ( !directive  ) continue; 
    var argName = 'arg' + directive.arg;
    argMap[argName] = argName + '=arguments[' + directive.arg + ']||[]';
    if ( directive.join != 'from' && directive.join != 'cross') {
      var indexField = directive.on[ directive.as ];
      var lookupName = 'p_' + argName + '_' + indexField;
      lookupMap[lookupName] = lookupName + '=' + 'pivot(' + argName + ',"' + indexField + '")';
    }
  }
  // emit argument uptake and lookup generation
  var j = 0;
  for( var i in argMap ) {
    if ( j++ == 0 ) code += 'var ';
    else code += eol + ',';
    code += argMap[i];
  }
  if ( j > 0 ) code += ';' + eol;
  
  j = 0;
  for( var i in lookupMap ) {
    if ( j++ == 0 ) code += 'var ';
    else code += eol + ',';
    code += lookupMap[i];
  }
  if ( j > 0 ) code += ';' + eol;

  // footers are used to 'close' the for loops
  var footers = [];
  
  // generate loop structure to perform join operation
  for( var i = 0, len = this.directives.length; i < len; i ++ ) {
    var directive = this.directives[i];
    if ( !directive  ) continue; 
    var valuelist_var = 'v' + i;
    var itr_var = 'i' + i;
    var itr_len_var = 'L' + i;
    
    if ( directive.join == 'left' ) {
      code += 'var ' + valuelist_var + '=' + DataJoin__getLookupCommand( directive ) + '||[{}];' + eol;
    } else if ( directive.join == 'inner' ) {
      code += 'var ' + valuelist_var + '=' + DataJoin__getLookupCommand( directive ) + '||[];' + eol;
    } else if ( directive.join == 'from' || directive.join == 'cross' ) {
      var argName = 'arg' + directive.arg;
      code += 'var ' + valuelist_var + '=' + argName + ';' + eol;
    }
    code += 'for(var ' + itr_var + '=0,' + itr_len_var + '=' + valuelist_var + '.length;' 
         + itr_var + '<' + itr_len_var + ';' + itr_var + '++){' + eol;
    code += 'x["' + directive.as + '"]=' + valuelist_var + '[' + itr_var + '];' + eol;

    // TODO: delete can be removed once directive dependency ordering is ensured
    footers.push( 'delete(x["' + directive.as + '"]);' + eol + '}' + eol );
  }
  
  // in the deepest loop, emit selector execution
  code += 'var sr=selector.call(x);' + eol;
  code += 'if(sr==x)sr=extend({},x);' + eol;
  code += 'if(sr)r.push(sr);' + eol;
  
  // emmit loop closing 'footers'
  while( footers.length > 0 ) {
    code += footers.pop();
  }
  
  // emit return statement - duh
  code += 'return r;';
  return code;
}

/**
 * computes the right hand side of an index based value!s! retrieval
 * for example the RHS of this statement: var joinWithMeList = lookup[..];
 */
function DataJoin__getLookupCommand( directive ) {
  if ( !directive.on ) return null;
  var argName = 'arg' + directive.arg;
  var indexField = directive.on[ directive.as ];
  var lookupName = 'p_' + argName + '_' + indexField;
  for ( var i in directive.on ) {
    if ( i != directive.as ) 
      return lookupName + '[x["' + i + '"]["' + directive.on[i] + '"]]';
  }
}


/**
 * Parses an SQL like FROM .. JOIN query fragment
 *
 * Groups:
 *   1 - function (from, left join, inner join, cross join, join)
 *   2 - argument number (numeric part of: arg0, arg1, ... 0, 1, ..,)
 *   3 - alias (RHS of AS)
 *   4 - join on LHS alias
 *   5 - join on LHS field
 *   6 - comparison operation (currently can only be '=')
 *   7 - join on RHS alias
 *   8 - join on RHS field
 */
var sqlFromJoinRegex = /(FROM|LEFT\s+JOIN|INNER\s+JOIN|CROSS JOIN|JOIN)\s+arg(\d+)\s+AS\s+([$a-zA-Z_][$a-zA-Z0-9_]*)\b(?:\s+ON\s+([$a-zA-Z_][$a-zA-Z0-9_]*)\.([$a-zA-Z_][$a-zA-Z0-9_]*)\s*(=)\s*([$a-zA-Z_][$a-zA-Z0-9_]*)\.([$a-zA-Z_][$a-zA-Z0-9_]*))?/ig;

/**
 * Parses an SQL like FROM .. JOIN query fragment into a set of directives
 * populates this.directives
 * 
 * Note that FROM and CROSS join will ignore any 'on' statements
 *
 * @param joinStr SQL join style string to parse
 * @return self for chaining
 */
DataJoin.prototype.parse = function( joinStr ) {
  this.directives = [];
  var match;
  var haveFromStatment = false;
  var aliasReuseCheck = {};
  
  while ( match = sqlFromJoinRegex.exec( joinStr ) ) {
    var join = match[1].toLowerCase();
    if ( /^(join|inner\s+join)$/.test( join ) ) join = 'inner';
    if ( /^left\s+join$/.test( join ) ) join = 'left';
    if ( /^cross\s+join$/.test( join ) ) join = 'cross';
    if ( join == 'from' ) {
      if ( haveFromStatment ) join = 'cross';
      haveFromStatment = true;
    }
    var directive = {
      join: join,
      arg: parseInt( match[2] ),
      as: match[3]
    };
    
    if ( aliasReuseCheck[directive.as] ) {
      throw new Error( "directive " + this.directives.length + " - alias '" + directive.as + "' cannot be used more than once" );
    }
    aliasReuseCheck[directive.as] = true;
    
    if ( !/from|cross/.test( join ) ) {
      if ( !match[4] || !match[5] || !match[7] || !match[8] )
        throw new Error( "directive " + this.directives.length + " - ON statment is missing or incorrectly formatted" );
      if ( match[4] != directive.as && match[7] != directive.as )
        throw new Error( "directive " + this.directives.length + " - ON statment must include a field from " + directive.as );
      if ( match[4] == match[7] )
        throw new Error( "directive " + this.directives.length + " - ON statment must include a field not from " + directive.as );
      
      directive.on = {};
      directive.on[ match[4] ] = match[5];
      directive.on[ match[7] ] = match[8];
    }
    this.directives.push( directive );
  }
  
  if ( !haveFromStatment ) {
    throw new Error( "FROM statment is required" );
  }
  return this;
}

DataManip.pivot = pivot;
DataManip.filter = filter;
DataManip.extend = extend;
DataManip.DataJoin = DataJoin;
if ( !window.DataJoin ) window.DataJoin = DataJoin;

})();
