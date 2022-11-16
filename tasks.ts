
import * as _ from "lodash";
import { Followee } from './types';

export function diff(v1: Followee[], v2: Followee[]) {

    const map1: { [k: string]: Followee } = v1.reduce(function (map, obj) {
        map[obj.id] = obj;
        return map;
    }, {} as { [k: string]: Followee });

    const map2 = v2.reduce(function (map, obj) {
        map[obj.id] = obj;
        return map;
    }, {} as { [k: string]: Followee });

    const ids1 = new Set(Object.keys(map1));
    const ids2 = new Set(Object.keys(map2));

    const deletedIds = new Set([...ids1].filter(x => !ids2.has(x)));
    const addedIds = new Set([...ids2].filter(x => !ids1.has(x)));

    let deleted = [...deletedIds].map((i) => {
        return map1[i];
    });
    let added = [...addedIds].map((i) => {
        return map2[i];
    });

    const deletedValues = Array.from(deleted.values());
    const addedValues = Array.from(added.values());

    return { deletedValues, addedValues};
}
