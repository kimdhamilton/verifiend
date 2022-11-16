export type Followee = {
    id: string,
    name: string,
    username: string
};

export type Status = {
    id: string,
    total: number,
    next: string | undefined
};

export type Paginator = {
    result_count: number,
    next_token: string,
    previous_token: string
}
