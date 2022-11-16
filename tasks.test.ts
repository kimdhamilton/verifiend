import {describe, expect, test} from '@jest/globals';
import {diff} from './tasks';

describe('tasks module', () => {
  test('doDiff basic test', () => {
    const v1 = [{id:"1",name:"Elion",username:"elion"}, {id:"2",name:"Scarramooch",username:"scara"}]
    const v2 = [{id:"1",name:"Elion",username:"elion"}, {id:"3",name:"Tarump",username:"tarump"}]

    const { deletedValues, addedValues } = diff(v1, v2);
    console.log(JSON.stringify(deletedValues));
    expect(deletedValues.length).toBe(1);
    expect(deletedValues[0]).toEqual({id: '2', name: 'Scarramooch', username: 'scara'});

    expect(addedValues.length).toBe(1);
    expect(addedValues[0]).toEqual({id: '3', name: 'Tarump', username: 'tarump'});
  });
});